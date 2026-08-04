package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avivklas/jaydb/pkg/storage"
)

// Item represents a cached object along with fetch timestamp.
type Item struct {
	Object    *storage.Object
	FetchedAt time.Time
}

// singleflightCall holds state for an in-flight cold storage fetch.
type singleflightCall struct {
	wg  sync.WaitGroup
	val *storage.Object
	err error
}

// Manager coordinates in-memory caching, key-level locking, and singleflight request coalescing.
type Manager struct {
	driver   storage.Driver
	mu       sync.RWMutex
	items    map[string]*Item
	sfCalls  map[string]*singleflightCall
	keyLocks sync.Map // map[string]*sync.Mutex
	ttl      time.Duration

	// Metrics counters
	hits   uint64
	misses uint64
	sfHits uint64
}

// NewManager initializes a cache manager backed by cold storage.
func NewManager(driver storage.Driver) *Manager {
	return &Manager{
		driver:  driver,
		items:   make(map[string]*Item),
		sfCalls: make(map[string]*singleflightCall),
		ttl:     5 * time.Second, // 5s default TTL for cache freshness and multi-node consistency
	}
}

// SetTTL sets the cache expiration duration.
func (m *Manager) SetTTL(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ttl = d
}

func (m *Manager) getKeyMutex(key string) *sync.Mutex {
	actual, _ := m.keyLocks.LoadOrStore(key, &sync.Mutex{})
	return actual.(*sync.Mutex)
}

// Get retrieves an object from cache or cold storage with singleflight coalescing.
func (m *Manager) Get(ctx context.Context, key string) (*storage.Object, error) {
	now := time.Now()

	// 1. Check cache under read lock
	m.mu.RLock()
	item, found := m.items[key]
	ttl := m.ttl
	m.mu.RUnlock()

	if found && ttl > 0 && now.Sub(item.FetchedAt) < ttl {
		atomic.AddUint64(&m.hits, 1)
		return item.Object, nil
	}

	atomic.AddUint64(&m.misses, 1)

	// 2. Coalesce cold-storage reads via singleflight
	m.mu.Lock()
	// Double check cache after acquiring write lock
	if item, found := m.items[key]; found && m.ttl > 0 && now.Sub(item.FetchedAt) < m.ttl {
		m.mu.Unlock()
		atomic.AddUint64(&m.hits, 1)
		return item.Object, nil
	}

	call, inFlight := m.sfCalls[key]
	if inFlight {
		m.mu.Unlock()
		atomic.AddUint64(&m.sfHits, 1)
		call.wg.Wait()
		return call.val, call.err
	}

	call = new(singleflightCall)
	call.wg.Add(1)
	m.sfCalls[key] = call
	m.mu.Unlock()

	// Execute single cold-storage fetch
	call.val, call.err = m.driver.Get(ctx, key)

	m.mu.Lock()
	delete(m.sfCalls, key)
	if call.err == nil && call.val != nil {
		m.items[key] = &Item{
			Object:    call.val,
			FetchedAt: time.Now(),
		}
	} else if call.err != nil {
		delete(m.items, key)
	}
	m.mu.Unlock()

	call.wg.Done()
	return call.val, call.err
}

// Put writes an object to cold storage under key-level mutex and populates the cache.
func (m *Manager) Put(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	keyLock := m.getKeyMutex(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	// Issue CAS write to cold storage
	obj, err := m.driver.Put(ctx, key, value, expectedETag)
	if err != nil {
		// Invalidate cache on failure
		m.Invalidate(key)
		return nil, err
	}

	// Update cache entry on success
	m.mu.Lock()
	m.items[key] = &Item{
		Object:    obj,
		FetchedAt: time.Now(),
	}
	m.mu.Unlock()

	return obj, nil
}

// Delete removes an object from cold storage and invalidates the cache.
func (m *Manager) Delete(ctx context.Context, key string, expectedETag string) error {
	keyLock := m.getKeyMutex(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	err := m.driver.Delete(ctx, key, expectedETag)
	m.Invalidate(key)
	return err
}

// Invalidate removes a key from the in-memory cache.
func (m *Manager) Invalidate(key string) {
	m.mu.Lock()
	delete(m.items, key)
	m.mu.Unlock()
}

// Stats returns cache hits, misses, and singleflight coalesced hit metrics.
func (m *Manager) Stats() (hits, misses, sfHits uint64) {
	return atomic.LoadUint64(&m.hits), atomic.LoadUint64(&m.misses), atomic.LoadUint64(&m.sfHits)
}
