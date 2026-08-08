package cache

import (
	"context"
	"fmt"
	"testing"

	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// BenchmarkParallelCacheHit measures cache-hit throughput under concurrency.
// The hot read path in production is ~4000 req/s of GETs across many keys, so
// per-op latency measured on a single goroutine hides the cost that matters:
// whether a hit serialises every other reader.
func BenchmarkParallelCacheHit(b *testing.B) {
	store := memory.NewDriver()
	mgr := NewManager(store)
	ctx := context.Background()

	const nKeys = 512
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench/key/%d", i)
		if _, err := mgr.Put(ctx, keys[i], []byte("cached-value-payload"), ""); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := mgr.Get(ctx, keys[i%nKeys]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
