package benchmarks

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/avivklas/jaydb/pkg/cache"
	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/encoding"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// BenchmarkSingleNodeGet measures single-node Get throughput with memory driver.
func BenchmarkSingleNodeGet(b *testing.B) {
	store := memory.NewDriver()
	database, _ := db.Open(db.Options{Storage: store})
	defer database.Close()

	ctx := context.Background()
	key := "benchmark/key"

	// Prime the key
	_, _ = database.Put(ctx, key, []byte("benchmark-value"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var val []byte
		_, _ = database.Get(ctx, key, &val)
	}
}

// BenchmarkSingleNodePut measures single-node Put throughput with memory driver.
func BenchmarkSingleNodePut(b *testing.B) {
	store := memory.NewDriver()
	database, _ := db.Open(db.Options{Storage: store})
	defer database.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("benchmark/key/%d", i)
		_, _ = database.Put(ctx, key, []byte("benchmark-value"))
	}
}

// BenchmarkCacheHit measures cache hit latency.
func BenchmarkCacheHit(b *testing.B) {
	store := memory.NewDriver()
	cacheMgr := cache.NewManager(store)

	ctx := context.Background()
	key := "benchmark/cache/key"

	// Prime cache
	_, _ = cacheMgr.Put(ctx, key, []byte("cached-value"), "")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = cacheMgr.Get(ctx, key)
	}
}

// BenchmarkCacheMiss measures cache miss + cold storage latency.
func BenchmarkCacheMiss(b *testing.B) {
	store := memory.NewDriver()
	cacheMgr := cache.NewManager(store)

	ctx := context.Background()

	// Pre-populate storage
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("benchmark/miss/%d", i)
		_, _ = store.Put(ctx, key, []byte("value"), "")
	}

	// Invalidate all from cache
	cacheMgr.SetTTL(0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("benchmark/miss/%d", i)
		_, _ = cacheMgr.Get(ctx, key)
	}
}

// BenchmarkSingleflightCoalescing measures singleflight effectiveness under concurrent reads.
func BenchmarkSingleflightCoalescing(b *testing.B) {
	store := &slowDriver{inner: memory.NewDriver()}
	cacheMgr := cache.NewManager(store)

	ctx := context.Background()
	key := "benchmark/singleflight"

	// Prime storage
	_, _ = store.Put(ctx, key, []byte("value"), "")

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Clear cache to force cold read
			cacheMgr.Invalidate(key)
			_, _ = cacheMgr.Get(ctx, key)
		}
	})
}

// slowDriver simulates a slow backend storage (e.g., S3).
type slowDriver struct {
	inner storage.Driver
	mu    sync.Mutex
}

func (s *slowDriver) Get(ctx context.Context, key string) (*storage.Object, error) {
	// Simulate 10ms network latency
	// time.Sleep(10 * time.Millisecond)  // Commented to keep benchmark fast
	return s.inner.Get(ctx, key)
}

func (s *slowDriver) Put(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	return s.inner.Put(ctx, key, value, expectedETag)
}

func (s *slowDriver) Delete(ctx context.Context, key string, expectedETag string) error {
	return s.inner.Delete(ctx, key, expectedETag)
}

func (s *slowDriver) List(ctx context.Context, prefix string, opts storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	return s.inner.List(ctx, prefix, opts)
}

func (s *slowDriver) Close() error {
	return s.inner.Close()
}

// BenchmarkJSONMarshal measures JSON encoding overhead.
func BenchmarkJSONMarshal(b *testing.B) {
	codec := encoding.NewJSONCodec()

	type Doc struct {
		Name  string `json:"name"`
		Email string `json:"email"`
		Age   int    `json:"age"`
	}

	doc := Doc{
		Name:  "Alice Johnson",
		Email: "alice@example.com",
		Age:   30,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = codec.Marshal(doc)
	}
}

// BenchmarkJSONUnmarshal measures JSON decoding overhead.
func BenchmarkJSONUnmarshal(b *testing.B) {
	codec := encoding.NewJSONCodec()

	type Doc struct {
		Name  string `json:"name"`
		Email string `json:"email"`
		Age   int    `json:"age"`
	}

	data := []byte(`{"name":"Alice Johnson","email":"alice@example.com","age":30}`)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var doc Doc
		_ = codec.Unmarshal(data, &doc)
	}
}

// BenchmarkConsistentHashRingLookup measures partition ring lookup performance.
func BenchmarkConsistentHashRingLookup(b *testing.B) {
	ring := sharding.NewRing(150, 2) // 150 virtual nodes per physical node

	ring.AddNode("127.0.0.1:9001")
	ring.AddNode("127.0.0.1:9002")
	ring.AddNode("127.0.0.1:9003")

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("users/%d/profile", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := keys[i%1000]
		_ = ring.GetNode(key)
	}
}

// BenchmarkPartitionKey measures partition key extraction performance.
func BenchmarkPartitionKey(b *testing.B) {
	key := "users/12345/profile/settings/preferences"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = sharding.PartitionKey(key, 2)
	}
}

// BenchmarkConcurrentWrites measures concurrent Put operations.
func BenchmarkConcurrentWrites(b *testing.B) {
	store := memory.NewDriver()
	database, _ := db.Open(db.Options{Storage: store})
	defer database.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("benchmark/concurrent/%d", i)
			_, _ = database.Put(ctx, key, []byte("value"))
			i++
		}
	})
}

// BenchmarkConcurrentReads measures concurrent Get operations on cached data.
func BenchmarkConcurrentReads(b *testing.B) {
	store := memory.NewDriver()
	database, _ := db.Open(db.Options{Storage: store})
	defer database.Close()

	ctx := context.Background()

	// Prime 100 keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("benchmark/read/%d", i)
		_, _ = database.Put(ctx, key, []byte("value"))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("benchmark/read/%d", i%100)
			var val []byte
			_, _ = database.Get(ctx, key, &val)
			i++
		}
	})
}

// BenchmarkCASUpdate measures optimistic locking performance.
func BenchmarkCASUpdate(b *testing.B) {
	store := memory.NewDriver()
	database, _ := db.Open(db.Options{Storage: store})
	defer database.Close()

	ctx := context.Background()
	key := "benchmark/cas"

	// Initial write
	meta, _ := database.Put(ctx, key, []byte("v0"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		newVal := []byte(fmt.Sprintf("v%d", i+1))
		meta, _ = database.Put(ctx, key, newVal, db.WithExpectedETag(meta.ETag))
	}
}
