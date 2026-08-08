package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/avivklas/jaydb/pkg/cache"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// TestCloseUnregistersCacheBudget pins the wiring that makes Budget.Unregister
// actually happen. A process hosting one DB per namespace creates and deletes
// namespaces at runtime; if Close leaves its cache Manager registered, the
// deleted namespace's cache is kept alive by the Budget, its bytes keep counting
// against the ceiling the surviving namespaces share, and Reserve's round-robin
// scan grows with every namespace ever opened.
func TestCloseUnregistersCacheBudget(t *testing.T) {
	ctx := context.Background()
	budget := cache.NewBudget(2000)

	deleted, err := Open(Options{Storage: memory.NewDriver(), CacheBudget: budget})
	if err != nil {
		t.Fatalf("open deleted: %v", err)
	}
	survivor, err := Open(Options{Storage: memory.NewDriver(), CacheBudget: budget})
	if err != nil {
		t.Fatalf("open survivor: %v", err)
	}
	defer survivor.Close()

	doc := map[string]string{"payload": strings.Repeat("x", 200)}
	for i := 0; i < 4; i++ {
		if _, err := deleted.Put(ctx, fmt.Sprintf("d/%d", i), doc); err != nil {
			t.Fatalf("deleted put %d: %v", i, err)
		}
	}
	if _, err := survivor.Put(ctx, "s/0", doc); err != nil {
		t.Fatalf("survivor put: %v", err)
	}

	_, survivorBytes := survivor.Cache().GetCacheSize()
	_, deletedBytes := deleted.Cache().GetCacheSize()
	if deletedBytes == 0 || survivorBytes == 0 {
		t.Fatalf("setup: expected both caches populated, got deleted=%d survivor=%d", deletedBytes, survivorBytes)
	}
	if used := budget.Used(); used != deletedBytes+survivorBytes {
		t.Fatalf("setup: budget used=%d, caches hold %d", used, deletedBytes+survivorBytes)
	}

	if err := deleted.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if used := budget.Used(); used != survivorBytes {
		t.Fatalf("after Close the budget still charges the deleted namespace: used=%d, want the survivor's %d",
			used, survivorBytes)
	}
	if items, bytes := deleted.Cache().GetCacheSize(); items != 0 || bytes != 0 {
		t.Fatalf("the closed database still holds %d items / %d bytes of cache", items, bytes)
	}

	// The closed database must also be out of the eviction rotation: filling the
	// ceiling from the survivor may only evict the survivor's own entries.
	deletedEvictionsBefore, _, _ := deleted.Cache().EvictionStats()
	for i := 1; i < 40; i++ {
		if _, err := survivor.Put(ctx, fmt.Sprintf("s/%d", i), doc); err != nil {
			t.Fatalf("survivor put %d: %v", i, err)
		}
	}
	if evictions, _, _ := survivor.Cache().EvictionStats(); evictions == 0 {
		t.Fatal("setup: the survivor never reached the ceiling, so nothing was under eviction pressure")
	}
	deletedEvictionsAfter, _, _ := deleted.Cache().EvictionStats()
	if deletedEvictionsAfter != deletedEvictionsBefore {
		t.Fatalf("the closed database was still used as an eviction source: %d -> %d evictions",
			deletedEvictionsBefore, deletedEvictionsAfter)
	}
}

// TestCloseWithoutBudgetStillCloses guards the nil case: an embedded database
// opened with no shared budget must close exactly as before.
func TestCloseWithoutBudgetStillCloses(t *testing.T) {
	ctx := context.Background()

	dbi, err := Open(Options{Storage: memory.NewDriver()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := dbi.Put(ctx, "k/1", map[string]int{"v": 1}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if dbi.Cache().Budget() != nil {
		t.Fatal("setup: expected no budget")
	}
	if err := dbi.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
