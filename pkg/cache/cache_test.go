package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

type countingDriver struct {
	storage.Driver
	getCalls uint64
	putCalls uint64
}

func (c *countingDriver) Get(ctx context.Context, key string) (*storage.Object, error) {
	atomic.AddUint64(&c.getCalls, 1)
	time.Sleep(20 * time.Millisecond) // Simulate S3 network latency
	return c.Driver.Get(ctx, key)
}

func (c *countingDriver) Put(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	atomic.AddUint64(&c.putCalls, 1)
	return c.Driver.Put(ctx, key, value, expectedETag)
}

func TestSingleflightCoalescing(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewDriver()

	// Seed cold storage
	_, err := mem.Put(ctx, "k1", []byte("cold-data"), "")
	if err != nil {
		t.Fatalf("failed to seed storage: %v", err)
	}

	cd := &countingDriver{Driver: mem}
	mgr := NewManager(cd)

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	results := make([][]byte, numGoroutines)
	errs := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		idx := i
		go func() {
			defer wg.Done()
			obj, err := mgr.Get(ctx, "k1")
			if obj != nil {
				results[idx] = obj.Value
			}
			errs[idx] = err
		}()
	}

	wg.Wait()

	// 1. Verify cold storage was hit EXACTLY 1 time
	getCalls := atomic.LoadUint64(&cd.getCalls)
	if getCalls != 1 {
		t.Fatalf("expected exactly 1 cold storage Get call, got %d", getCalls)
	}

	// 2. Verify all 100 callers got the correct payload
	for i := 0; i < numGoroutines; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d returned error: %v", i, errs[i])
		}
		if string(results[i]) != "cold-data" {
			t.Fatalf("goroutine %d got '%s', expected 'cold-data'", i, string(results[i]))
		}
	}

	// 3. Verify subsequent read is a pure cache hit (0 additional cold storage calls)
	obj, err := mgr.Get(ctx, "k1")
	if err != nil || string(obj.Value) != "cold-data" {
		t.Fatalf("cached Get failed: %v", err)
	}
	if atomic.LoadUint64(&cd.getCalls) != 1 {
		t.Fatalf("expected getCalls to remain 1 after cache hit")
	}
}

func TestWritePopulationAndInvalidation(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewDriver()
	cd := &countingDriver{Driver: mem}
	mgr := NewManager(cd)

	// 1. Write populates cache immediately
	obj1, err := mgr.Put(ctx, "users/1", []byte("alice"), storage.MatchAnyETag)
	if err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}

	// 2. Immediate read hits cache without touching cold storage Get
	readObj, err := mgr.Get(ctx, "users/1")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if string(readObj.Value) != "alice" {
		t.Fatalf("expected 'alice', got '%s'", readObj.Value)
	}
	if atomic.LoadUint64(&cd.getCalls) != 0 {
		t.Fatalf("expected 0 cold storage Get calls after Put, got %d", cd.getCalls)
	}

	// 3. Failed CAS write invalidates cache
	_, err = mgr.Put(ctx, "users/1", []byte("bob"), `"wrong-etag"`)
	if err != storage.ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// 4. Update with correct ETag
	_, err = mgr.Put(ctx, "users/1", []byte("bob"), obj1.ETag)
	if err != nil {
		t.Fatalf("unexpected update error: %v", err)
	}

	readObj2, err := mgr.Get(ctx, "users/1")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if string(readObj2.Value) != "bob" {
		t.Fatalf("expected 'bob', got '%s'", readObj2.Value)
	}
}
