package cache

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// seedManager returns a Manager over an in-memory driver with a TTL long enough
// that nothing expires mid-test.
func seedManager(t *testing.T, cfg Config) (*Manager, storage.Driver) {
	t.Helper()
	if cfg.TTL == 0 {
		cfg.TTL = time.Hour
	}
	mem := memory.NewDriver()
	return NewManagerWithConfig(mem, cfg), mem
}

func TestBudgetReserveEvictsToFit(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(300)
	mgr, _ := seedManager(t, Config{Budget: budget})

	for i := 0; i < 3; i++ {
		if _, err := mgr.Put(ctx, fmt.Sprintf("k%d", i), make([]byte, 100), ""); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	items, bytes := mgr.GetCacheSize()
	if items != 3 || bytes != 300 {
		t.Fatalf("expected 3 items / 300 bytes at the ceiling, got %d / %d", items, bytes)
	}

	// The fourth entry cannot fit without eviction.
	if _, err := mgr.Put(ctx, "k3", make([]byte, 100), ""); err != nil {
		t.Fatalf("put k3: %v", err)
	}

	items, bytes = mgr.GetCacheSize()
	if items != 3 || bytes != 300 {
		t.Fatalf("expected the budget to hold at 3 items / 300 bytes, got %d / %d", items, bytes)
	}
	if used := budget.Used(); used != 300 {
		t.Fatalf("expected budget used 300, got %d", used)
	}
	if evictions, _, _ := mgr.EvictionStats(); evictions != 1 {
		t.Fatalf("expected exactly 1 eviction, got %d", evictions)
	}
}

func TestBudgetLimitZeroIsUnboundedButTracks(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(0)
	mgr, _ := seedManager(t, Config{Budget: budget})

	for i := 0; i < 10; i++ {
		if _, err := mgr.Put(ctx, fmt.Sprintf("k%d", i), make([]byte, 50), ""); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if items, _ := mgr.GetCacheSize(); items != 10 {
		t.Fatalf("an unbounded budget must not evict, got %d items", items)
	}
	if used := budget.Used(); used != 500 {
		t.Fatalf("expected tracked usage 500 even when unbounded, got %d", used)
	}
	if budget.Limit() != 0 {
		t.Fatalf("expected limit 0, got %d", budget.Limit())
	}
}

func TestBudgetEvictsRoundRobinAcrossManagers(t *testing.T) {
	ctx := context.Background()
	// Room for four 100-byte entries.
	budget := NewBudget(400)
	a, _ := seedManager(t, Config{Budget: budget})
	b, _ := seedManager(t, Config{Budget: budget})

	for i := 0; i < 2; i++ {
		if _, err := a.Put(ctx, fmt.Sprintf("a%d", i), make([]byte, 100), ""); err != nil {
			t.Fatalf("a put: %v", err)
		}
		if _, err := b.Put(ctx, fmt.Sprintf("b%d", i), make([]byte, 100), ""); err != nil {
			t.Fatalf("b put: %v", err)
		}
	}

	// Two more inserts force two evictions. A round-robin cursor takes one from
	// each Manager; a fixed victim would empty a first.
	for i := 2; i < 4; i++ {
		if _, err := a.Put(ctx, fmt.Sprintf("a%d", i), make([]byte, 100), ""); err != nil {
			t.Fatalf("a put: %v", err)
		}
	}

	aEvictions, _, _ := a.EvictionStats()
	bEvictions, _, _ := b.EvictionStats()
	if aEvictions != 1 || bEvictions != 1 {
		t.Fatalf("expected one eviction from each Manager, got a=%d b=%d", aEvictions, bEvictions)
	}
	if used := budget.Used(); used != 400 {
		t.Fatalf("expected budget pinned at 400, got %d", used)
	}
}

func TestBudgetReserveFailsWhenNothingLeftToEvict(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(100)
	mgr, mem := seedManager(t, Config{Budget: budget})

	// A single object larger than the whole budget can never fit, so it must be
	// returned to the caller uncached rather than breaching the ceiling.
	obj, err := mgr.Put(ctx, "big", make([]byte, 500), "")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if obj == nil || len(obj.Value) != 500 {
		t.Fatalf("caller must still receive the object")
	}
	if items, bytes := mgr.GetCacheSize(); items != 0 || bytes != 0 {
		t.Fatalf("expected nothing cached, got %d items / %d bytes", items, bytes)
	}
	if used := budget.Used(); used != 0 {
		t.Fatalf("a failed Reserve must not leak bytes, got used=%d", used)
	}

	// And the document is still readable from cold storage.
	got, err := mem.Get(ctx, "big")
	if err != nil || len(got.Value) != 500 {
		t.Fatalf("cold storage read failed: %v", err)
	}
}

// TestBudgetSharedConcurrentManagers is the lock-ordering regression test: the
// Budget mutex is held across EvictOldest, which takes a Manager mutex, so a
// Manager that ever called a Budget method while holding its own mutex would
// deadlock here.
func TestBudgetSharedConcurrentManagers(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(64 * 1024)

	const (
		managers   = 2
		goroutines = 16
		iterations = 200
	)

	mgrs := make([]*Manager, managers)
	for i := range mgrs {
		mgrs[i], _ = seedManager(t, Config{Budget: budget, MaxObjectSize: 4096})
	}

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for mi, mgr := range mgrs {
			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func(mgr *Manager, mi, g int) {
					defer wg.Done()
					for i := 0; i < iterations; i++ {
						key := fmt.Sprintf("m%d/g%d/k%d", mi, g, i%32)
						switch i % 4 {
						case 0:
							_, _ = mgr.Put(ctx, key, make([]byte, 512), "")
						case 1:
							_, _ = mgr.Get(ctx, key)
						case 2:
							mgr.Invalidate(key)
						default:
							mgr.PurgeIf(func(k string) bool {
								return strings.HasSuffix(k, "k7")
							})
						}
					}
				}(mgr, mi, g)
			}
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("timed out: Managers and Budget deadlocked")
	}

	if used := budget.Used(); used < 0 || used > budget.Limit() {
		t.Fatalf("budget accounting drifted: used=%d limit=%d", used, budget.Limit())
	}

	// Accounting must still agree with what is actually held.
	var total int64
	for _, mgr := range mgrs {
		_, bytes := mgr.GetCacheSize()
		total += bytes
	}
	if total != budget.Used() {
		t.Fatalf("budget used=%d but Managers hold %d bytes", budget.Used(), total)
	}
}

func TestLRUEvictionOrder(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(300)
	mgr, _ := seedManager(t, Config{Budget: budget})

	for _, k := range []string{"a", "b", "c"} {
		if _, err := mgr.Put(ctx, k, make([]byte, 100), ""); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// Touch "a" so "b" becomes the least recently used.
	if _, err := mgr.Get(ctx, "a"); err != nil {
		t.Fatalf("get a: %v", err)
	}

	if _, err := mgr.Put(ctx, "d", make([]byte, 100), ""); err != nil {
		t.Fatalf("put d: %v", err)
	}

	if got := cachedKeys(mgr); !equalKeySet(got, []string{"a", "c", "d"}) {
		t.Fatalf("expected the least recently used entry (b) to go, cache holds %v", got)
	}
}

func TestMaxObjectSizeSkipsCaching(t *testing.T) {
	ctx := context.Background()
	mgr, _ := seedManager(t, Config{MaxObjectSize: 128})

	small, err := mgr.Put(ctx, "small", make([]byte, 64), "")
	if err != nil {
		t.Fatalf("put small: %v", err)
	}
	big, err := mgr.Put(ctx, "big", make([]byte, 256), "")
	if err != nil {
		t.Fatalf("put big: %v", err)
	}
	if big == nil || len(big.Value) != 256 {
		t.Fatal("an oversized object must still be returned to the caller")
	}

	if got := cachedKeys(mgr); !equalKeySet(got, []string{"small"}) {
		t.Fatalf("expected only the small object cached, got %v", got)
	}
	if _, skipped, _ := mgr.EvictionStats(); skipped != 1 {
		t.Fatalf("expected 1 skipped-large, got %d", skipped)
	}

	// Growing an existing entry past the cap must drop the stale small version
	// rather than leave it behind to be served.
	if _, err := mgr.Put(ctx, "small", make([]byte, 512), small.ETag); err != nil {
		t.Fatalf("grow small: %v", err)
	}
	if items, bytes := mgr.GetCacheSize(); items != 0 || bytes != 0 {
		t.Fatalf("expected the stale entry dropped, got %d items / %d bytes", items, bytes)
	}
}

func TestGetCacheSizeAccounting(t *testing.T) {
	ctx := context.Background()
	mgr, _ := seedManager(t, Config{})

	obj, err := mgr.Put(ctx, "k", make([]byte, 100), "")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if items, bytes := mgr.GetCacheSize(); items != 1 || bytes != 100 {
		t.Fatalf("after insert: got %d / %d, want 1 / 100", items, bytes)
	}

	// Overwrite with a smaller payload: the running total must follow, not add.
	if _, err := mgr.Put(ctx, "k", make([]byte, 40), obj.ETag); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if items, bytes := mgr.GetCacheSize(); items != 1 || bytes != 40 {
		t.Fatalf("after overwrite: got %d / %d, want 1 / 40", items, bytes)
	}

	mgr.Invalidate("k")
	if items, bytes := mgr.GetCacheSize(); items != 0 || bytes != 0 {
		t.Fatalf("after invalidate: got %d / %d, want 0 / 0", items, bytes)
	}
}

func TestPurgeIfDropsMatchingAndReleasesBudget(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(10000)
	mgr, _ := seedManager(t, Config{Budget: budget})

	for _, k := range []string{"keep/1", "drop/1", "keep/2", "drop/2"} {
		if _, err := mgr.Put(ctx, k, make([]byte, 100), ""); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	dropped := mgr.PurgeIf(func(key string) bool { return strings.HasPrefix(key, "drop/") })
	if dropped != 2 {
		t.Fatalf("expected 2 dropped, got %d", dropped)
	}
	if got := cachedKeys(mgr); !equalKeySet(got, []string{"keep/1", "keep/2"}) {
		t.Fatalf("expected only keep/* retained, got %v", got)
	}
	if used := budget.Used(); used != 200 {
		t.Fatalf("expected 200 bytes still reserved, got %d", used)
	}
	if _, _, purged := mgr.EvictionStats(); purged != 2 {
		t.Fatalf("expected purged counter 2, got %d", purged)
	}
}

// blockingDriver holds every Get until release is closed, so a test can keep a
// singleflight leader parked while it inspects follower behaviour.
type blockingDriver struct {
	storage.Driver
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingDriver) Get(ctx context.Context, key string) (*storage.Object, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.Driver.Get(ctx, key)
}

// TestSingleflightFollowerHonoursContext covers the bug where a follower did a
// bare wait on the leader: one slow S3 fetch pinned every concurrent reader of
// the key with no way out.
func TestSingleflightFollowerHonoursContext(t *testing.T) {
	mem := memory.NewDriver()
	if _, err := mem.Put(context.Background(), "k", []byte("cold"), ""); err != nil {
		t.Fatalf("seed: %v", err)
	}

	drv := &blockingDriver{
		Driver:  mem,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	mgr := NewManagerWithConfig(drv, Config{TTL: time.Hour})

	leaderDone := make(chan error, 1)
	go func() {
		_, err := mgr.Get(context.Background(), "k")
		leaderDone <- err
	}()

	// Wait until the leader is actually inside the driver call.
	select {
	case <-drv.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("leader never reached the driver")
	}

	followerCtx, cancel := context.WithCancel(context.Background())
	followerDone := make(chan error, 1)
	go func() {
		_, err := mgr.Get(followerCtx, "k")
		followerDone <- err
	}()

	// Give the follower time to attach to the in-flight call, then kill it.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-followerDone:
		if err != context.Canceled {
			t.Fatalf("follower should return context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("follower did not return while the leader was still blocked")
	}

	// The leader must still complete and populate the cache for everyone else.
	close(drv.release)
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader: %v", err)
	}
	if items, _ := mgr.GetCacheSize(); items != 1 {
		t.Fatalf("expected the leader to populate the cache, got %d items", items)
	}
	if _, _, sfHits := mgr.Stats(); sfHits != 1 {
		t.Fatalf("expected 1 singleflight follower, got %d", sfHits)
	}
}

// cachedKeys reports what the cache currently holds across shards.
func cachedKeys(m *Manager) []string {
	var keys []string
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		for el := shard.lru.Front(); el != nil; el = el.Next() {
			keys = append(keys, el.Value.(*Item).key)
		}
		shard.mu.RUnlock()
	}
	return keys
}

func equalKeySet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, k := range got {
		seen[k] = true
	}
	for _, k := range want {
		if !seen[k] {
			return false
		}
	}
	return true
}
