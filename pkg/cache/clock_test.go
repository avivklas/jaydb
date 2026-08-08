package cache

import (
	"context"
	"fmt"
	"testing"
)

// TestEvictionMakesProgressWhenEveryEntryIsReferenced covers the CLOCK edge
// case: if every entry has been read since the last sweep, a naive
// second-chance loop spares them all and frees nothing, so Reserve would fail
// and the cache would stop admitting anything. The sweep must still produce a
// victim.
func TestEvictionMakesProgressWhenEveryEntryIsReferenced(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(300)
	mgr, _ := seedManager(t, Config{Budget: budget})

	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		if _, err := mgr.Put(ctx, k, make([]byte, 100), ""); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// Reference every entry, so no entry is a free victim.
	for _, k := range keys {
		if _, err := mgr.Get(ctx, k); err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
	}

	freed, ok := mgr.EvictOldest()
	if !ok {
		t.Fatal("EvictOldest reported nothing to evict while 3 entries were cached")
	}
	if freed != 100 {
		t.Fatalf("freed = %d, want 100", freed)
	}
	if items, _ := mgr.GetCacheSize(); items != 2 {
		t.Fatalf("cache holds %d items after one eviction, want 2", items)
	}
}

// TestAdmitStillSucceedsUnderSustainedReads pushes the same scenario through the
// real admission path: a full cache whose entries are all hot must still accept
// new keys rather than silently refusing to cache anything.
func TestAdmitStillSucceedsUnderSustainedReads(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(500)
	mgr, _ := seedManager(t, Config{Budget: budget})

	for i := 0; i < 5; i++ {
		if _, err := mgr.Put(ctx, fmt.Sprintf("k%d", i), make([]byte, 100), ""); err != nil {
			t.Fatalf("seed put: %v", err)
		}
	}

	for round := 0; round < 3; round++ {
		// Make everything hot, then admit a new key.
		for i := 0; i < 5; i++ {
			_, _ = mgr.Get(ctx, fmt.Sprintf("k%d", i))
		}
		newKey := fmt.Sprintf("new%d", round)
		if _, err := mgr.Put(ctx, newKey, make([]byte, 100), ""); err != nil {
			t.Fatalf("put %s: %v", newKey, err)
		}
		if !contains(cachedKeys(mgr), newKey) {
			t.Fatalf("round %d: %s was not admitted; cache holds %v", round, newKey, cachedKeys(mgr))
		}
	}

	if used, limit := budget.Used(), budget.Limit(); used > limit {
		t.Fatalf("budget breached: used %d > limit %d", used, limit)
	}
}

func contains(keys []string, want string) bool {
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}
