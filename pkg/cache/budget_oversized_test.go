package cache

import (
	"context"
	"testing"
)

// TestBudgetRefusesOversizedWithoutDraining pins the fix for a defect where a
// single object larger than the whole ceiling destroyed every cache in the
// process. Reserve's eviction loop only stopped once every registered Evictor
// reported empty, so an unfittable object drained all of them and then STILL
// failed to admit - a self-inflicted cold-cache stampede across every namespace,
// repeated on the next request for that key.
//
// The existing TestBudgetReserveFailsWhenNothingLeftToEvict looks like it covers
// this but exercises an empty cache, where the loop exits after one refusal. The
// non-empty case is the only one with a consequence.
func TestBudgetRefusesOversizedWithoutDraining(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(300)

	// Two Managers sharing the budget, as a multi-namespace process has.
	m1, _ := seedManager(t, Config{Budget: budget})
	m2, _ := seedManager(t, Config{Budget: budget})

	for _, k := range []string{"a", "b"} {
		if _, err := m1.Put(ctx, k, make([]byte, 75), ""); err != nil {
			t.Fatalf("m1 put %s: %v", k, err)
		}
		if _, err := m2.Put(ctx, k, make([]byte, 75), ""); err != nil {
			t.Fatalf("m2 put %s: %v", k, err)
		}
	}

	items1Before, bytes1Before := m1.GetCacheSize()
	items2Before, bytes2Before := m2.GetCacheSize()
	usedBefore := budget.Used()
	if items1Before != 2 || items2Before != 2 {
		t.Fatalf("setup: expected 2 entries each, got %d and %d", items1Before, items2Before)
	}

	// No per-object cap (MaxObjectSize 0 is a documented no-cap), so this reaches
	// the budget: 1000 bytes against a 300-byte ceiling can never fit.
	if _, err := m1.Put(ctx, "huge", make([]byte, 1000), ""); err != nil {
		t.Fatalf("put huge: %v", err)
	}

	items1After, bytes1After := m1.GetCacheSize()
	items2After, bytes2After := m2.GetCacheSize()

	if items1After != items1Before || bytes1After != bytes1Before {
		t.Errorf("oversized object disturbed its own Manager: %d items/%dB -> %d items/%dB",
			items1Before, bytes1Before, items1After, bytes1After)
	}
	if items2After != items2Before || bytes2After != bytes2Before {
		t.Errorf("oversized object evicted an UNRELATED Manager's entries: %d items/%dB -> %d items/%dB",
			items2Before, bytes2Before, items2After, bytes2After)
	}
	if used := budget.Used(); used != usedBefore {
		t.Errorf("budget usage changed: %d -> %d", usedBefore, used)
	}

	// The object itself must not be cached, and must not be left half-accounted.
	if m1.contains("huge") {
		t.Error("an object larger than the entire budget was cached")
	}
}

// TestBudgetStillEvictsWhenObjectCanFit guards the other side of the refusal: an
// object that DOES fit within the ceiling must still trigger eviction rather than
// being refused outright.
func TestBudgetStillEvictsWhenObjectCanFit(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(300)
	mgr, _ := seedManager(t, Config{Budget: budget})

	for _, k := range []string{"a", "b", "c"} {
		if _, err := mgr.Put(ctx, k, make([]byte, 100), ""); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// 200 bytes fits the 300-byte ceiling, so two entries should be evicted.
	if _, err := mgr.Put(ctx, "d", make([]byte, 200), ""); err != nil {
		t.Fatalf("put d: %v", err)
	}

	if !mgr.contains("d") {
		t.Fatal("an object that fits the ceiling was refused")
	}
	if used, limit := budget.Used(), budget.Limit(); used > limit {
		t.Fatalf("budget breached: used %d > limit %d", used, limit)
	}
}

// TestAdmittedSizeSurvivesCallerMutation pins the accounting fix. Get hands the
// cached *storage.Object straight to the caller (GetRaw does this across the
// cluster mesh), so recomputing an entry's size from Object.Value at eviction
// time let a caller reslicing that value corrupt the budget - releasing more or
// less than was reserved. The admitted size is recorded on the entry instead.
func TestAdmittedSizeSurvivesCallerMutation(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(10000)
	mgr, _ := seedManager(t, Config{Budget: budget})

	obj, err := mgr.Put(ctx, "k", make([]byte, 500), "")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if used := budget.Used(); used != 500 {
		t.Fatalf("reserved %d bytes, want 500", used)
	}

	// A caller mutating what it was handed must not be able to move the budget.
	obj.Value = obj.Value[:10]

	mgr.Invalidate("k")

	if used := budget.Used(); used != 0 {
		t.Fatalf("budget leaked after a caller resliced the returned object: used=%d, want 0", used)
	}
	if _, bytes := mgr.GetCacheSize(); bytes != 0 {
		t.Fatalf("cache byte total drifted: %d, want 0", bytes)
	}
}
