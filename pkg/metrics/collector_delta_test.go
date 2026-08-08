package metrics

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()

	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}

	return m.GetCounter().GetValue()
}

// TestUpdateCacheMetricsAddsDeltasNotTotals pins the counter-sync contract.
// cache.Manager reports running totals; the collector previously added the
// whole total on every sync, so a cache with 100 hits scraped three times
// reported 300. Nothing in jaydb-cloud consumed these metrics before, which is
// why it went unnoticed.
func TestUpdateCacheMetricsAddsDeltasNotTotals(t *testing.T) {
	var hits, misses, sfHits uint64

	c := NewCollector(
		func() (uint64, uint64, uint64) { return hits, misses, sfHits },
		nil,
	)

	before := counterValue(t, CacheHits)

	hits, misses, sfHits = 100, 10, 5
	c.UpdateCacheMetrics()
	if got := counterValue(t, CacheHits) - before; got != 100 {
		t.Fatalf("after first sync CacheHits advanced by %v, want 100", got)
	}

	// Second sync with no new activity must not move the counter.
	c.UpdateCacheMetrics()
	if got := counterValue(t, CacheHits) - before; got != 100 {
		t.Fatalf("idle sync re-added the total: advanced by %v, want 100", got)
	}

	hits = 250
	c.UpdateCacheMetrics()
	if got := counterValue(t, CacheHits) - before; got != 250 {
		t.Fatalf("after third sync CacheHits advanced by %v, want 250", got)
	}
}

// TestUpdateCacheMetricsIsConcurrencySafe pins the fix for a data race in the
// delta bookkeeping. UpdateCacheMetrics is exported and the package directs
// callers to invoke it from a /metrics handler, where overlapping scrapes are
// normal - two Prometheus servers, or a scrape racing a manual curl. Unguarded,
// two calls each computed a delta from the same `last` and added the same
// increment twice, permanently over-reporting the counter that this delta
// tracking exists to keep honest. Run under -race to catch the regression.
func TestUpdateCacheMetricsIsConcurrencySafe(t *testing.T) {
	var counter atomic.Uint64

	c := NewCollector(
		func() (uint64, uint64, uint64) {
			v := counter.Add(1)
			return v, v, v
		},
		func() (int, int64) { return 1, 1 },
	).WithEvictionStats(func() (uint64, uint64, uint64) {
		v := counter.Load()
		return v, v, v
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				c.UpdateCacheMetrics()
			}
		}()
	}
	wg.Wait()
}

// TestDeltaHandlesSourceReset guards the unsigned-underflow trap: a source that
// goes backwards must contribute 0, not 2^64 minus the difference.
func TestDeltaHandlesSourceReset(t *testing.T) {
	last := uint64(500)

	if got := delta(&last, 100); got != 0 {
		t.Fatalf("delta on a reset source = %d, want 0", got)
	}
	if last != 100 {
		t.Fatalf("delta did not rebase last to 100, got %d", last)
	}
	if got := delta(&last, 150); got != 50 {
		t.Fatalf("delta after rebase = %d, want 50", got)
	}
}
