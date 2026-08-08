package db

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/cache"
	"github.com/avivklas/jaydb/pkg/cluster"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// reconcileCountingDriver counts cold-storage reads so a test can tell whether a
// key is still cached without reaching into the cache's internals.
type reconcileCountingDriver struct {
	storage.Driver
	getCalls atomic.Uint64
}

func (c *reconcileCountingDriver) Get(ctx context.Context, key string) (*storage.Object, error) {
	c.getCalls.Add(1)
	return c.Driver.Get(ctx, key)
}

// newClusteredDB builds a single-node cluster on ephemeral ports plus a database
// wired to its ring, and returns the concrete type so the reconcile path can be
// driven directly.
func newClusteredDB(t *testing.T) (*database, *reconcileCountingDriver, *sharding.Ring, string) {
	t.Helper()

	ring := sharding.NewRing(100, 2)
	drv := &reconcileCountingDriver{Driver: memory.NewDriver()}

	node, err := cluster.NewNode(cluster.NodeConfig{
		NodeName: fmt.Sprintf("reconcile-%s", t.Name()),
		BindAddr: "127.0.0.1",
		BindPort: 0,
		QuicPort: 0,
		Ring:     ring,
	})
	if err != nil {
		t.Fatalf("cluster node: %v", err)
	}

	dbi, err := Open(Options{
		Storage:     drv,
		Ring:        ring,
		ClusterNode: node,
		// Long TTL so nothing expires on its own: this test is about ownership
		// invalidation, which is what should let the TTL be raised at all.
		CacheTTL: time.Hour,
	})
	if err != nil {
		_ = node.Close()
		t.Fatalf("open: %v", err)
	}
	// database.Close also closes the ClusterNode it was handed.
	t.Cleanup(func() { _ = dbi.Close() })

	return dbi.(*database), drv, ring, node.SelfQuicAddr()
}

// pickPeer returns a peer address whose addition splits keys between self and
// somebody else. The node's own QUIC port is ephemeral, so which keys move
// depends on an address the test does not choose - picking the peer against a
// scratch ring keeps the split deterministic instead of leaving it to luck.
// Ring ownership is a pure function of (members, replicas, depth), so the
// scratch ring's verdict holds for the real one.
func pickPeer(t *testing.T, existing, keys []string) string {
	t.Helper()

	self := existing[0]
	inUse := make(map[string]bool, len(existing))
	for _, addr := range existing {
		inUse[addr] = true
	}

	for port := 59000; port < 59200; port++ {
		cand := fmt.Sprintf("127.0.0.1:%d", port)
		// Re-proposing a current member would be a no-op on the real ring, and
		// no-op adds deliberately do not bump the generation.
		if inUse[cand] {
			continue
		}

		scratch := sharding.NewRing(100, 2)
		for _, addr := range existing {
			scratch.AddNode(addr)
		}
		scratch.AddNode(cand)

		var moved, retained int
		for _, k := range keys {
			if scratch.GetNode(k) == self {
				retained++
			} else {
				moved++
			}
		}
		if moved > 0 && retained > 0 {
			return cand
		}
	}

	t.Fatal("no candidate peer produced a mixed ownership split")
	return ""
}

// splitByOwner returns which of keys the ring assigns elsewhere and which it
// still assigns to self.
func splitByOwner(t *testing.T, ring *sharding.Ring, self string, keys []string) (moved, retained []string) {
	t.Helper()
	for _, k := range keys {
		if ring.GetNode(k) == self {
			retained = append(retained, k)
		} else {
			moved = append(moved, k)
		}
	}
	if len(moved) == 0 || len(retained) == 0 {
		t.Fatalf("degenerate split: %d moved, %d retained - the test needs both", len(moved), len(retained))
	}
	return moved, retained
}

func seedCache(t *testing.T, d *database, keys []string) {
	t.Helper()
	for _, k := range keys {
		// Written straight through the cache manager: routing a peer-owned key
		// through database.Put would send it over the mesh, and what this test
		// cares about is the contents of the local cache.
		if _, err := d.cacheMgr.Put(context.Background(), k, []byte("v"), ""); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}
}

func testKeys(n int) []string {
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		keys = append(keys, fmt.Sprintf("tenant/%d/doc", i))
	}
	return keys
}

func TestReconcileOwnershipPurgesMovedKeys(t *testing.T) {
	ctx := context.Background()
	d, drv, ring, self := newClusteredDB(t)

	keys := testKeys(60)
	seedCache(t, d, keys)

	// Settle the generation the node started at, so the purge under test is
	// caused by the second node joining and nothing else.
	d.reconcileOwnership()
	if items, _ := d.cacheMgr.GetCacheSize(); items != len(keys) {
		t.Fatalf("a single-member ring owns every key, expected %d cached, got %d", len(keys), items)
	}

	peer := pickPeer(t, []string{self}, keys)
	ring.AddNode(peer)
	moved, retained := splitByOwner(t, ring, self, keys)
	t.Logf("second node %s took %d of %d keys", peer, len(moved), len(keys))
	// Any operation is enough to trigger the lazy reconcile; use a key this node
	// still owns so it stays on the local path.
	if _, err := d.Get(ctx, retained[0], nil); err != nil {
		t.Fatalf("get retained: %v", err)
	}

	if items, _ := d.cacheMgr.GetCacheSize(); items != len(retained) {
		t.Fatalf("expected %d entries to survive the purge, got %d", len(retained), items)
	}
	if _, _, purged := d.cacheMgr.EvictionStats(); purged != uint64(len(moved)) {
		t.Fatalf("expected %d entries purged, got %d", len(moved), purged)
	}

	// Prove identity rather than just counts: a moved key must now cost a cold
	// read, a retained key must not.
	before := drv.getCalls.Load()
	if _, err := d.cacheMgr.Get(ctx, retained[1]); err != nil {
		t.Fatalf("get retained from cache: %v", err)
	}
	if got := drv.getCalls.Load(); got != before {
		t.Fatalf("retained key %q was purged: it triggered a cold read", retained[1])
	}
	if _, err := d.cacheMgr.Get(ctx, moved[0]); err != nil {
		t.Fatalf("get moved from storage: %v", err)
	}
	if got := drv.getCalls.Load(); got != before+1 {
		t.Fatalf("moved key %q was still cached: no cold read happened", moved[0])
	}
}

func TestReconcileRunsOncePerGeneration(t *testing.T) {
	ctx := context.Background()
	d, _, ring, self := newClusteredDB(t)

	keys := testKeys(60)
	seedCache(t, d, keys)
	d.reconcileOwnership()

	peer := pickPeer(t, []string{self}, keys)
	ring.AddNode(peer)
	moved, retained := splitByOwner(t, ring, self, keys)

	if _, err := d.Get(ctx, retained[0], nil); err != nil {
		t.Fatalf("get: %v", err)
	}
	_, _, purgedAfterFirst := d.cacheMgr.EvictionStats()
	if purgedAfterFirst != uint64(len(moved)) {
		t.Fatalf("expected %d purged on the generation change, got %d", len(moved), purgedAfterFirst)
	}

	// Put the moved entries back and hammer the DB at the SAME generation. A
	// reconcile per request would purge them all over again.
	seedCache(t, d, moved)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = d.Get(ctx, retained[0], nil)
		}()
	}
	wg.Wait()

	if _, _, purged := d.cacheMgr.EvictionStats(); purged != purgedAfterFirst {
		t.Fatalf("reconcile re-ran at an unchanged generation: purged went %d -> %d", purgedAfterFirst, purged)
	}
	if items, _ := d.cacheMgr.GetCacheSize(); items != len(keys) {
		t.Fatalf("expected all %d re-seeded entries to survive, got %d", len(keys), items)
	}

	// A real membership change must still trigger exactly one more purge.
	peer2 := pickPeer(t, []string{self, peer}, keys)
	ring.AddNode(peer2)
	movedAgain, retainedAgain := splitByOwner(t, ring, self, keys)

	for i := 0; i < 10; i++ {
		if _, err := d.Get(ctx, retainedAgain[0], nil); err != nil {
			t.Fatalf("get after second change: %v", err)
		}
	}

	// purged is cumulative, so the second purge shows up as an increment.
	_, _, purgedAfterSecond := d.cacheMgr.EvictionStats()
	if want := purgedAfterFirst + uint64(len(movedAgain)); purgedAfterSecond != want {
		t.Fatalf("expected cumulative purged %d after the second change, got %d", want, purgedAfterSecond)
	}
	if items, _ := d.cacheMgr.GetCacheSize(); items != len(retainedAgain) {
		t.Fatalf("expected %d entries retained, got %d", len(retainedAgain), items)
	}
}

func TestReconcileNoopWithoutCluster(t *testing.T) {
	ctx := context.Background()
	drv := &reconcileCountingDriver{Driver: memory.NewDriver()}

	dbi, err := Open(Options{Storage: drv, CacheTTL: time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer dbi.Close()

	d := dbi.(*database)
	seedCache(t, d, testKeys(10))

	if _, err := d.Get(ctx, "tenant/0/doc", nil); err != nil {
		t.Fatalf("get: %v", err)
	}

	// An embedded database has no ring, so nothing may ever be purged.
	if _, _, purged := d.cacheMgr.EvictionStats(); purged != 0 {
		t.Fatalf("embedded database purged %d entries", purged)
	}
	if items, _ := d.cacheMgr.GetCacheSize(); items != 10 {
		t.Fatalf("expected 10 cached entries, got %d", items)
	}
}

func TestOpenPassesCacheOptions(t *testing.T) {
	ctx := context.Background()
	budget := cache.NewBudget(300)

	dbi, err := Open(Options{
		Storage:            memory.NewDriver(),
		CacheTTL:           time.Hour,
		CacheMaxObjectSize: 64,
		CacheBudget:        budget,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer dbi.Close()

	// Marshalled JSON for this doc is well over the 64-byte cap.
	big := map[string]string{"blob": string(make([]byte, 200))}
	if _, err := dbi.Put(ctx, "big/1", big); err != nil {
		t.Fatalf("put: %v", err)
	}

	if _, skipped, _ := dbi.Cache().EvictionStats(); skipped != 1 {
		t.Fatalf("expected the per-object cap to reject the document, skipped=%d", skipped)
	}
	if used := budget.Used(); used != 0 {
		t.Fatalf("nothing should be reserved, got used=%d", used)
	}
}
