package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeEvictor is a scriptable eviction source holding a fixed number of
// equal-sized entries. Real Managers cannot show WHICH Evictor the round-robin
// cursor picked - only a per-Evictor eviction count - so the victim order, the
// thing an off-by-one cursor corrupts, needs a source that records every call.
type fakeEvictor struct {
	name string
	unit int64

	mu    sync.Mutex
	held  int64
	calls int

	order *evictionOrder
}

// evictionOrder is the shared victim log. Separate from fakeEvictor.mu so the
// order across Evictors is recorded under one lock.
type evictionOrder struct {
	mu  sync.Mutex
	log []string
}

func (o *evictionOrder) record(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.log = append(o.log, name)
}

func (o *evictionOrder) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.log...)
}

func (f *fakeEvictor) EvictOldest() (int64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	if f.held < f.unit {
		return 0, false
	}
	f.held -= f.unit
	if f.order != nil {
		f.order.record(f.name)
	}

	return f.unit, true
}

func (f *fakeEvictor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// stockEvictors registers len(names) Evictors holding entries each, reserving
// every entry through the budget so Used() reflects what they hold.
func stockEvictors(t *testing.T, b *Budget, unit int64, entries int, names ...string) []*fakeEvictor {
	t.Helper()

	order := &evictionOrder{}
	out := make([]*fakeEvictor, 0, len(names))
	for _, name := range names {
		f := &fakeEvictor{name: name, unit: unit, order: order}
		b.Register(f)
		for i := 0; i < entries; i++ {
			if !b.Reserve(unit) {
				t.Fatalf("setup: reserving entry %d for %s was refused", i, name)
			}
			f.held += unit
		}
		out = append(out, f)
	}

	return out
}

// TestBudgetUnregisterStopsEvicting pins the primary consequence: a departed
// Manager must never be asked for memory again, because the process has dropped
// it and its entries are gone.
func TestBudgetUnregisterStopsEvicting(t *testing.T) {
	budget := NewBudget(500)
	ev := stockEvictors(t, budget, 100, 2, "gone", "stays")
	gone, stays := ev[0], ev[1]

	budget.Unregister(gone)
	callsAtDeparture := gone.callCount()

	// 200 bytes on top of the 200 the survivor holds fits under the 500 ceiling
	// only after something is evicted.
	if !budget.Reserve(400) {
		t.Fatalf("reserve was refused with %d of 500 bytes used", budget.Used())
	}

	if got := gone.callCount(); got != callsAtDeparture {
		t.Fatalf("Budget evicted from an unregistered Evictor: %d calls at departure, %d after",
			callsAtDeparture, got)
	}
	if stays.callCount() == 0 {
		t.Fatal("the surviving Evictor was never asked for room")
	}
}

// TestBudgetUnregisterReleasesDepartedBytes pins the ceiling consequence: bytes
// held by a deleted namespace must stop counting immediately, not linger until
// some later reservation happens to evict them one entry at a time. Uses a real
// Manager so the Manager-side accounting is exercised too.
func TestBudgetUnregisterReleasesDepartedBytes(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(600)
	gone, _ := seedManager(t, Config{Budget: budget})
	stays, _ := seedManager(t, Config{Budget: budget})

	for i := 0; i < 3; i++ {
		if _, err := gone.Put(ctx, fmt.Sprintf("g%d", i), make([]byte, 100), ""); err != nil {
			t.Fatalf("gone put %d: %v", i, err)
		}
	}
	if _, err := stays.Put(ctx, "s0", make([]byte, 100), ""); err != nil {
		t.Fatalf("stays put: %v", err)
	}
	if used := budget.Used(); used != 400 {
		t.Fatalf("setup: expected 400 bytes used, got %d", used)
	}

	budget.Unregister(gone)

	if used := budget.Used(); used != 100 {
		t.Fatalf("expected only the survivor's 100 bytes to count after Unregister, got %d", used)
	}
	if items, bytes := gone.GetCacheSize(); items != 0 || bytes != 0 {
		t.Fatalf("the departed Manager still holds %d items / %d bytes", items, bytes)
	}

	// The freed room must be genuinely usable by a live namespace rather than
	// merely unaccounted.
	for i := 1; i < 5; i++ {
		if _, err := stays.Put(ctx, fmt.Sprintf("s%d", i), make([]byte, 100), ""); err != nil {
			t.Fatalf("stays put %d: %v", i, err)
		}
	}
	if evictions, _, _ := stays.EvictionStats(); evictions != 0 {
		t.Fatalf("the survivor was squeezed by the departed namespace's bytes: %d evictions", evictions)
	}
	if used := budget.Used(); used != 500 {
		t.Fatalf("expected 500 bytes used, got %d", used)
	}
}

// TestBudgetUnregisterNeverDoubleSubtracts covers the other half of the release
// invariant. Whichever order a caller uses - purge then unregister, or
// unregister then purge - the departed bytes must be subtracted exactly once.
// Releasing them without also dropping the entries that own them is the trap:
// the Manager's own later Invalidate/PurgeIf then releases the same bytes again,
// Used() is clamped at zero and the SURVIVING namespaces silently lose their
// accounting.
func TestBudgetUnregisterNeverDoubleSubtracts(t *testing.T) {
	orders := []struct {
		name  string
		apply func(t *testing.T, budget *Budget, gone *Manager)
	}{
		{
			name: "purge then unregister",
			apply: func(t *testing.T, budget *Budget, gone *Manager) {
				gone.PurgeIf(func(string) bool { return true })
				budget.Unregister(gone)
				budget.Unregister(gone)
			},
		},
		{
			name: "unregister then purge",
			apply: func(t *testing.T, budget *Budget, gone *Manager) {
				budget.Unregister(gone)
				// A straggler request or a belt-and-braces caller purging after
				// the fact must find nothing left to release.
				gone.PurgeIf(func(string) bool { return true })
				gone.Invalidate("g0")
			},
		},
	}

	for _, order := range orders {
		t.Run(order.name, func(t *testing.T) {
			ctx := context.Background()
			budget := NewBudget(1000)
			gone, _ := seedManager(t, Config{Budget: budget})
			stays, _ := seedManager(t, Config{Budget: budget})

			for i := 0; i < 3; i++ {
				if _, err := gone.Put(ctx, fmt.Sprintf("g%d", i), make([]byte, 100), ""); err != nil {
					t.Fatalf("gone put %d: %v", i, err)
				}
			}
			if _, err := stays.Put(ctx, "s0", make([]byte, 100), ""); err != nil {
				t.Fatalf("stays put: %v", err)
			}

			order.apply(t, budget, gone)

			if used := budget.Used(); used != 100 {
				t.Fatalf("used=%d, want the survivor's 100 - the departed bytes were not subtracted exactly once", used)
			}
			if _, bytes := stays.GetCacheSize(); bytes != 100 {
				t.Fatalf("the survivor's cache was disturbed: %d bytes", bytes)
			}
		})
	}
}

// TestBudgetUnregisterIsIdempotent pins that a teardown path may call this more
// than once, or with something that was never registered, without a panic.
func TestBudgetUnregisterIsIdempotent(t *testing.T) {
	budget := NewBudget(500)
	ev := stockEvictors(t, budget, 100, 1, "a", "b")

	budget.Unregister(ev[0])
	usedAfterFirst := budget.Used()
	callsAfterFirst := ev[0].callCount()

	budget.Unregister(ev[0])
	if used := budget.Used(); used != usedAfterFirst {
		t.Fatalf("a second Unregister changed accounting: %d -> %d", usedAfterFirst, used)
	}
	if got := ev[0].callCount(); got != callsAfterFirst {
		t.Fatalf("a second Unregister re-drained the Evictor: %d -> %d calls", callsAfterFirst, got)
	}

	// Never registered, and nil.
	unknown := &fakeEvictor{name: "unknown", unit: 100}
	budget.Unregister(unknown)
	budget.Unregister(nil)
	if unknown.callCount() != 0 {
		t.Fatal("Unregister drained an Evictor that was never registered")
	}
	if used := budget.Used(); used != usedAfterFirst {
		t.Fatalf("unregistering an unknown Evictor changed accounting: %d -> %d", usedAfterFirst, used)
	}

	// The survivor must still be reachable and usable as an eviction source.
	if !budget.Reserve(450) {
		t.Fatalf("reserve refused after the removals: used=%d", budget.Used())
	}
	if ev[1].callCount() == 0 {
		t.Fatal("the surviving Evictor was lost by the removal")
	}
}

// TestBudgetUnregisterClearsBackingArraySlot pins the leak itself. Shrinking the
// slice with a plain copy leaves a stale Evictor pointer in the vacated tail
// slot: when the removed element WAS the tail, that pointer is the departed
// Manager, so the Budget keeps its whole cache reachable and the memory is never
// returned - the exact symptom this change exists to fix, and invisible to every
// behavioural assertion.
func TestBudgetUnregisterClearsBackingArraySlot(t *testing.T) {
	cases := []struct {
		name   string
		remove int
	}{
		{name: "removing the tail", remove: 2},
		{name: "removing the middle", remove: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			budget := NewBudget(1000)
			ev := stockEvictors(t, budget, 100, 1, "a", "b", "c")
			departed := ev[tc.remove]

			budget.Unregister(departed)

			budget.mu.Lock()
			defer budget.mu.Unlock()

			full := budget.evictors[:cap(budget.evictors)]
			for i, e := range full {
				if i >= len(budget.evictors) && e != nil {
					t.Errorf("slot %d past the end of the slice still references %v", i, e)
				}
				if e == Evictor(departed) {
					t.Errorf("the departed Evictor is still reachable from slot %d of the backing array", i)
				}
			}
		})
	}
}

// TestBudgetUnregisterKeepsCursorValid is where an off-by-one hides. Removing an
// element shifts every later index down by one, so a cursor left alone either
// skips the Evictor that moved into the vacated slot or, when it was the last
// index, indexes past the end of the slice and panics inside Reserve.
func TestBudgetUnregisterKeepsCursorValid(t *testing.T) {
	cases := []struct {
		name   string
		cursor int
		remove string
		want   []string
	}{
		{
			// cursor past the removal point must follow it down, or the Evictor
			// that shifted into the vacated slot is skipped.
			name:   "cursor after removal follows",
			cursor: 2,
			remove: "b",
			want:   []string{"c", "a", "c"},
		},
		{
			// cursor at the removal point already points at the successor.
			name:   "cursor at removal takes successor",
			cursor: 1,
			remove: "b",
			want:   []string{"c", "a", "c"},
		},
		{
			// cursor at the last index with that index removed must wrap, or
			// Reserve indexes out of range.
			name:   "cursor at removed tail wraps",
			cursor: 2,
			remove: "c",
			want:   []string{"a", "b", "a"},
		},
		{
			name:   "cursor before removal is unchanged",
			cursor: 0,
			remove: "c",
			want:   []string{"a", "b", "a"},
		},
	}

	const unit = int64(100)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Used sits exactly at the ceiling, so every further Reserve(unit)
			// forces exactly one eviction and lands back at the ceiling.
			budget := NewBudget(unit * 9)
			ev := stockEvictors(t, budget, unit, 3, "a", "b", "c")

			byName := map[string]*fakeEvictor{}
			for _, f := range ev {
				byName[f.name] = f
			}

			budget.mu.Lock()
			budget.cursor = tc.cursor
			budget.mu.Unlock()

			departed := byName[tc.remove]
			budget.Unregister(departed)
			callsAtDeparture := departed.callCount()

			// The drain freed the departed Evictor's bytes, so top the budget
			// back up to the ceiling - a Reserve that fits changes nothing and
			// would leave the cursor untouched, proving nothing.
			if slack := budget.Limit() - budget.Used(); slack > 0 {
				if !budget.Reserve(slack) {
					t.Fatalf("topping the budget back to the ceiling was refused: used=%d", budget.Used())
				}
			}

			for i := 0; i < len(tc.want); i++ {
				if !budget.Reserve(unit) {
					t.Fatalf("reserve %d refused: used=%d limit=%d", i, budget.Used(), budget.Limit())
				}
			}

			// The drain writes the departed Evictor's own entries to the shared
			// log, so compare only the victims chosen after it left.
			got := ev[0].order.snapshot()
			if len(got) < len(tc.want) {
				t.Fatalf("only %d evictions recorded, want at least %d: %v", len(got), len(tc.want), got)
			}
			got = got[len(got)-len(tc.want):]
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("round-robin order after removing %s with cursor %d: got %v, want %v",
						tc.remove, tc.cursor, got, tc.want)
				}
			}
			if n := departed.callCount(); n != callsAtDeparture {
				t.Fatalf("the departed Evictor was asked for room %d more times", n-callsAtDeparture)
			}
		})
	}
}

// TestBudgetUnregisterConcurrent is the lock-ordering and race regression test:
// Unregister drains the departing Manager while holding Budget.mu, exactly as
// Reserve does, so a Manager that ever reached for the Budget while holding its
// own mutex - or an Unregister that took the locks the other way round - would
// deadlock here. The watchdog turns that deadlock into a failure instead of a
// hung test run.
func TestBudgetUnregisterConcurrent(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(64 * 1024)

	const (
		managers   = 4
		writers    = 8
		iterations = 150
	)

	mgrs := make([]*Manager, managers)
	for i := range mgrs {
		mgrs[i], _ = seedManager(t, Config{Budget: budget, MaxObjectSize: 4096})
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		var wg sync.WaitGroup
		for mi, mgr := range mgrs {
			for g := 0; g < writers; g++ {
				wg.Add(1)
				go func(mgr *Manager, mi, g int) {
					defer wg.Done()
					for i := 0; i < iterations; i++ {
						key := fmt.Sprintf("m%d/g%d/k%d", mi, g, i%16)
						switch i % 3 {
						case 0:
							_, _ = mgr.Put(ctx, key, make([]byte, 512), "")
						case 1:
							_, _ = mgr.Get(ctx, key)
						default:
							mgr.Invalidate(key)
						}
					}
				}(mgr, mi, g)
			}
		}

		// Churn membership underneath the writers, the way a process that
		// creates and deletes namespaces at runtime does.
		for _, mgr := range mgrs[2:] {
			wg.Add(1)
			go func(mgr *Manager) {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					budget.Unregister(mgr)
					budget.Register(mgr)
				}
			}(mgr)
		}

		// Independent reservation pressure, so eviction runs against a slice
		// that is being mutated.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations*4; i++ {
				if budget.Reserve(1024) {
					budget.Release(1024)
				}
			}
		}()

		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("timed out: Unregister deadlocked against Reserve/EvictOldest")
	}

	// Quiescent: every byte reserved must be held by exactly one Manager,
	// whether or not it is still registered.
	var held int64
	for _, mgr := range mgrs {
		_, bytes := mgr.GetCacheSize()
		held += bytes
	}
	if used := budget.Used(); used != held {
		t.Fatalf("budget accounting drifted under concurrent unregistration: used=%d, Managers hold %d", used, held)
	}
	if used, limit := budget.Used(), budget.Limit(); used < 0 || used > limit {
		t.Fatalf("budget out of range: used=%d limit=%d", used, limit)
	}
}
