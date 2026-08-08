package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// keyN builds a partitioned key. Sharding depth is 2, so the first two segments
// decide ownership - varying the second segment is what spreads keys across the
// ring.
func keyN(i int) string {
	return fmt.Sprintf("tenant/%d/doc", i)
}

// TestRawMethodsReconcileOwnership pins the fix for the most consequential hole
// in the ownership purge: the cluster mesh serves every FORWARDED request through
// GetRaw/PutRaw/DeleteRaw, and those bypassed reconcileOwnership entirely. The
// forwarded path is the only path that exists in a cluster, and reconciling on
// the node that forwards does nothing for the stale entry sitting in the cache of
// the node that receives it. Without the fix, GetRaw returns the stale value and
// the driver is never consulted.
func TestRawMethodsReconcileOwnership(t *testing.T) {
	ctx := context.Background()
	dbi, drv, ring, self := newClusteredDB(t)

	keys := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		keys = append(keys, keyN(i))
	}

	for _, k := range keys {
		if _, err := dbi.PutRaw(ctx, k, []byte(`{"v":1}`), ""); err != nil {
			t.Fatalf("seed put %s: %v", k, err)
		}
	}

	// Everything is cached, so a raw read must not touch cold storage.
	before := drv.getCalls.Load()
	for _, k := range keys {
		if _, err := dbi.GetRaw(ctx, k); err != nil {
			t.Fatalf("warm get %s: %v", k, err)
		}
	}
	if got := drv.getCalls.Load(); got != before {
		t.Fatalf("warm GetRaw hit cold storage %d times, want 0", got-before)
	}

	// Move ownership of some keys away from this node.
	peer := pickPeer(t, []string{self}, keys)
	ring.AddNode(peer)
	moved, retained := splitByOwner(t, ring, self, keys)
	if len(moved) == 0 || len(retained) == 0 {
		t.Fatalf("expected a mixed split, moved=%d retained=%d", len(moved), len(retained))
	}

	// A single GetRaw must trigger the purge for the whole generation.
	if _, err := dbi.GetRaw(ctx, retained[0]); err != nil {
		t.Fatalf("get after ring change: %v", err)
	}

	// Every key this node no longer owns must be gone from the cache, so reading
	// it goes back to cold storage.
	for _, k := range moved {
		fetches := drv.getCalls.Load()
		if _, err := dbi.GetRaw(ctx, k); err != nil {
			t.Fatalf("get moved key %s: %v", k, err)
		}
		if drv.getCalls.Load() == fetches {
			t.Fatalf("moved key %s was served from a stale cache entry; GetRaw did not reconcile ownership", k)
		}
	}
}

// TestReconcileFullPurgeOnSkippedGenerations covers the away-and-back case. A
// selective purge only reasons about the CURRENT ring, so a key that moved to
// another node, was written there, and moved back is owned by this node again at
// reconcile time - it passes the owner==self test and its pre-move value
// survives. Skipping generations is the only observable signal that this could
// have happened, so more than one elapsed generation must drop everything.
func TestReconcileFullPurgeOnSkippedGenerations(t *testing.T) {
	ctx := context.Background()
	dbi, drv, ring, self := newClusteredDB(t)

	keys := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		keys = append(keys, keyN(i))
	}
	for _, k := range keys {
		if _, err := dbi.Put(ctx, k, map[string]int{"v": 1}); err != nil {
			t.Fatalf("seed put %s: %v", k, err)
		}
	}
	// Establish a reconciled generation before the churn.
	if _, err := dbi.Get(ctx, keys[0], &map[string]int{}); err != nil {
		t.Fatalf("warm get: %v", err)
	}

	genBefore := ring.Generation()

	// Two membership changes: a key can have moved away and come back, and this
	// node observed neither transition.
	peer := pickPeer(t, []string{self}, keys)
	ring.AddNode(peer)
	ring.RemoveNode(peer)

	if got := ring.Generation(); got != genBefore+2 {
		t.Fatalf("expected two generation bumps, went %d -> %d", genBefore, got)
	}

	// After the churn every key is owned by self again (the peer is gone), so a
	// selective purge would retain all of them. The full purge must not.
	for _, k := range keys {
		if owner := ring.GetNode(k); owner != self {
			t.Fatalf("expected self to own %s again after the peer left, got %q", k, owner)
		}
	}

	if _, err := dbi.Get(ctx, keys[0], &map[string]int{}); err != nil {
		t.Fatalf("get after churn: %v", err)
	}

	// Reading any key must now go back to cold storage: the cache was dropped.
	for _, k := range keys[1:] {
		fetches := drv.getCalls.Load()
		if _, err := dbi.Get(ctx, k, &map[string]int{}); err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if drv.getCalls.Load() == fetches {
			t.Fatalf("key %s survived a two-generation churn; a stale value can be served after an away-and-back move", k)
		}
	}
}

// TestReconcileEscalatesWhenRingMovesDuringPurge covers the residual race a
// verification pass found in the skipped-generation fix. The selective predicate
// reads the ring LIVE, once per key, so a membership change landing mid-purge
// means the ownership answers did not all come from one generation: a key can
// move away, be written on its new owner, and move back before the predicate
// reaches it, at which point it reports self as owner and the pre-move value
// survives. Worse, the generation stored afterwards makes the NEXT reconcile see
// a single clean step, so it selectively purges and retains the stale entry
// again - permanently, until the TTL.
//
// The fix escalates to a full purge when the generation moved during a selective
// purge. Driven here through the test seam, because the window is otherwise not
// deterministically reachable.
func TestReconcileEscalatesWhenRingMovesDuringPurge(t *testing.T) {
	ctx := context.Background()
	dbi, drv, ring, self := newClusteredDB(t)

	keys := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		keys = append(keys, keyN(i))
	}
	for _, k := range keys {
		if _, err := dbi.Put(ctx, k, map[string]int{"v": 1}); err != nil {
			t.Fatalf("seed put %s: %v", k, err)
		}
	}
	// Settle a reconciled generation so the next change is a single clean step.
	if _, err := dbi.Get(ctx, keys[0], &map[string]int{}); err != nil {
		t.Fatalf("warm get: %v", err)
	}

	peer := pickPeer(t, []string{self}, keys)

	// One membership change: gen == last+1, so reconcile takes the SELECTIVE
	// path. The seam then removes the peer, which is the mid-purge movement.
	ring.AddNode(peer)

	var fired int
	dbi.hookAfterSelectivePurge = func() {
		fired++
		ring.RemoveNode(peer)
	}

	if _, err := dbi.Get(ctx, keys[0], &map[string]int{}); err != nil {
		t.Fatalf("get during churn: %v", err)
	}
	dbi.hookAfterSelectivePurge = nil

	if fired != 1 {
		t.Fatalf("the selective path did not run (hook fired %d times); the test is not exercising the race", fired)
	}

	// Every key is owned by self again now the peer is gone, so a selective purge
	// would have retained all of them. The escalation must have dropped the lot.
	for _, k := range keys {
		if owner := ring.GetNode(k); owner != self {
			t.Fatalf("expected self to own %s after the peer left, got %q", k, owner)
		}
	}

	// keys[0] is excluded: the request that triggered the purge went on to fetch
	// and re-admit it from cold storage, so its presence is correct.
	for _, k := range keys[1:] {
		fetches := drv.getCalls.Load()
		if _, err := dbi.Get(ctx, k, &map[string]int{}); err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if drv.getCalls.Load() == fetches {
			t.Fatalf("key %s survived a ring change that landed mid-purge; a stale value can be served indefinitely", k)
		}
	}
}

// TestOpenPassesCacheTTL closes a gap in TestOpenPassesCacheOptions, which set
// CacheTTL but asserted only on the cap and the budget - it passed whether or not
// the TTL reached the Manager. A one-nanosecond TTL expires deterministically, so
// every read after the write must miss; with the wiring dropped the 5s package
// default applies and both reads would hit.
func TestOpenPassesCacheTTL(t *testing.T) {
	ctx := context.Background()

	dbi, err := Open(Options{
		Storage:  memory.NewDriver(),
		CacheTTL: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer dbi.Close()

	if _, err := dbi.Put(ctx, "k/1", map[string]int{"v": 1}); err != nil {
		t.Fatalf("put: %v", err)
	}

	_, missesBefore, _ := dbi.Cache().Stats()
	for i := 0; i < 2; i++ {
		if _, err := dbi.Get(ctx, "k/1", &map[string]int{}); err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
	}
	_, missesAfter, _ := dbi.Cache().Stats()

	if got := missesAfter - missesBefore; got != 2 {
		t.Fatalf("with a 1ns TTL both reads must miss, got %d misses - CacheTTL did not reach the cache", got)
	}
}

// TestOpenDefaultsMatchNewManager pins the load-bearing half of the
// backward-compatibility claim: a zero-valued Options must behave exactly as
// before this change, i.e. the cache package default TTL, no cap, no budget.
func TestOpenDefaultsMatchNewManager(t *testing.T) {
	ctx := context.Background()

	dbi, err := Open(Options{Storage: memory.NewDriver()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer dbi.Close()

	if b := dbi.Cache().Budget(); b != nil {
		t.Fatalf("a zero-valued Options must not attach a budget, got %#v", b)
	}

	if _, err := dbi.Put(ctx, "k/1", map[string]int{"v": 1}); err != nil {
		t.Fatalf("put: %v", err)
	}

	// A large document must still be cached: the default is no per-object cap.
	big := map[string]string{"blob": string(make([]byte, 1<<20))}
	if _, err := dbi.Put(ctx, "big/1", big); err != nil {
		t.Fatalf("put big: %v", err)
	}
	if _, skipped, _ := dbi.Cache().EvictionStats(); skipped != 0 {
		t.Fatalf("default config must not cap object size, skipped=%d", skipped)
	}

	// And the default TTL must be long enough that an immediate re-read hits.
	hitsBefore, _, _ := dbi.Cache().Stats()
	if _, err := dbi.Get(ctx, "k/1", &map[string]int{}); err != nil {
		t.Fatalf("get: %v", err)
	}
	if hits, _, _ := dbi.Cache().Stats(); hits != hitsBefore+1 {
		t.Fatal("an immediate re-read should hit under the default TTL")
	}
}
