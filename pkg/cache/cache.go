package cache

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avivklas/jaydb/pkg/storage"
)

// defaultTTL keeps cache entries fresh enough for multi-node consistency when
// ownership churn is the only other coherence mechanism.
const defaultTTL = 5 * time.Second

// Item represents a cached object along with fetch timestamp.
type Item struct {
	Object    *storage.Object
	FetchedAt time.Time

	// key lets an eviction reach the map entry from the LRU list element it
	// found, without a reverse scan.
	key string

	// size is the byte count reserved against the Budget when this entry was
	// admitted. Every decrement and release uses THIS value rather than
	// re-measuring Object.Value, because GetRaw hands the cached *storage.Object
	// straight to the caller: if that caller reslices Value, a recomputed size
	// would not match what was reserved and the budget would leak or
	// over-release. Immutable for the lifetime of the entry.
	size int64

	// ref is the CLOCK reference bit, set by a cache hit and cleared by the
	// eviction sweep. Recency is tracked with this bit rather than by moving
	// the entry to the front of the list because reordering needs the write
	// lock: doing that on every hit serialised all concurrent readers and cost
	// ~50% of hit throughput at 4-8 goroutines. Atomic so the hit path can set
	// it while holding only a read lock.
	ref atomic.Bool
}

// singleflightCall holds state for an in-flight cold storage fetch.
type singleflightCall struct {
	// done is closed by the leader once val and err are final. A channel
	// rather than a WaitGroup so followers can select against their own
	// context instead of waiting unconditionally.
	done chan struct{}
	val  *storage.Object
	err  error
}

// Config configures a cache Manager.
type Config struct {
	// TTL is the entry freshness window. Zero means the package default.
	TTL time.Duration

	// MaxObjectSize refuses to cache objects larger than this many bytes; they
	// are still returned to the caller. Zero means no cap. One multi-megabyte
	// document should not be able to flush an entire working set.
	MaxObjectSize int64

	// Budget is the byte accountant, typically shared with the other Managers
	// in the process. Nil means unbounded and unshared.
	Budget *Budget
}

// Manager coordinates in-memory caching, key-level locking, and singleflight request coalescing.
type Manager struct {
	driver storage.Driver

	mu sync.RWMutex
	// items maps key -> LRU list element whose Value is the *Item, so a hit can
	// reorder in O(1).
	items map[string]*list.Element
	// lru is ordered most-recently-used at the front; eviction takes the back.
	lru     *list.List
	sfCalls map[string]*singleflightCall
	// bytes is a running total of cached payload sizes. GetCacheSize used to
	// sum the whole map on every metrics scrape, which stalled every request
	// behind the lock.
	bytes int64

	keyLocks sync.Map // map[string]*sync.Mutex
	ttl      time.Duration

	maxObjectSize int64

	// budget is shared with the other Managers in this process. NEVER call a
	// method on it while holding m.mu: Budget.Reserve calls EvictOldest, which
	// takes m.mu itself. See the lock-ordering note on Budget.
	budget *Budget

	// Metrics counters
	hits   uint64
	misses uint64
	sfHits uint64

	evictions    uint64
	skippedLarge uint64
	purged       uint64
}

// NewManager initializes a cache manager backed by cold storage, with the
// package default TTL, no per-object cap and no shared budget.
func NewManager(driver storage.Driver) *Manager {
	return NewManagerWithConfig(driver, Config{})
}

// NewManagerWithConfig initializes a cache manager from an explicit config.
// Zero values reproduce NewManager's behaviour exactly.
func NewManagerWithConfig(driver storage.Driver, cfg Config) *Manager {
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = defaultTTL
	}

	m := &Manager{
		driver:        driver,
		items:         make(map[string]*list.Element),
		lru:           list.New(),
		sfCalls:       make(map[string]*singleflightCall),
		ttl:           ttl,
		maxObjectSize: cfg.MaxObjectSize,
		budget:        cfg.Budget,
	}

	if cfg.Budget != nil {
		cfg.Budget.Register(m)
	}

	return m
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

func objectSize(obj *storage.Object) int64 {
	if obj == nil {
		return 0
	}
	return int64(len(obj.Value))
}

// Get retrieves an object from cache or cold storage with singleflight coalescing.
func (m *Manager) Get(ctx context.Context, key string) (*storage.Object, error) {
	// 1. Cache lookup on the read lock. A hit records recency by setting the
	// entry's CLOCK bit, which needs no exclusive access - see Item.ref.
	m.mu.RLock()
	ttl := m.ttl
	if el, found := m.items[key]; found {
		item := el.Value.(*Item)
		if ttl > 0 && time.Since(item.FetchedAt) < ttl {
			obj := item.Object
			item.ref.Store(true)
			m.mu.RUnlock()
			atomic.AddUint64(&m.hits, 1)
			return obj, nil
		}
	}
	m.mu.RUnlock()

	atomic.AddUint64(&m.misses, 1)

	// 2. Coalesce cold-storage reads via singleflight.
	m.mu.Lock()
	// Re-check under the write lock: another goroutine may have admitted this
	// key between the read unlock above and here.
	if el, found := m.items[key]; found {
		item := el.Value.(*Item)
		if ttl > 0 && time.Since(item.FetchedAt) < ttl {
			obj := item.Object
			item.ref.Store(true)
			m.mu.Unlock()
			atomic.AddUint64(&m.hits, 1)
			return obj, nil
		}
	}

	call, inFlight := m.sfCalls[key]
	if inFlight {
		m.mu.Unlock()
		atomic.AddUint64(&m.sfHits, 1)
		return awaitCall(ctx, call)
	}

	call = &singleflightCall{done: make(chan struct{})}
	m.sfCalls[key] = call
	m.mu.Unlock()

	// Execute single cold-storage fetch. Both the wakeup and the map cleanup are
	// deferred: if driver.Get panics, closing done alone would release followers
	// but leave the call in m.sfCalls forever, so every later request for that
	// key would join a finished call and get its zero (nil, nil) result.
	defer func() {
		m.mu.Lock()
		delete(m.sfCalls, key)
		m.mu.Unlock()
		close(call.done)
	}()
	call.val, call.err = m.driver.Get(ctx, key)

	if call.err == nil && call.val != nil {
		m.admit(key, call.val)
	} else if call.err != nil {
		m.Invalidate(key)
	}

	return call.val, call.err
}

// awaitCall waits for the leader's fetch to finish, but only as long as the
// follower's own context lives. A bare wait let one slow leader - an S3 fetch
// behind a stalled connection - pin every concurrent reader of the key with no
// way to time out. The leader still runs to completion and populates the cache
// for everyone else.
func awaitCall(ctx context.Context, call *singleflightCall) (*storage.Object, error) {
	select {
	case <-call.done:
		return call.val, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// admit caches obj under key, subject to the per-object cap and the shared
// budget. The budget reservation deliberately happens BEFORE m.mu is taken:
// Reserve may call EvictOldest on this very Manager, which locks m.mu itself.
func (m *Manager) admit(key string, obj *storage.Object) {
	if obj == nil {
		return
	}

	size := objectSize(obj)
	if m.maxObjectSize > 0 && size > m.maxObjectSize {
		atomic.AddUint64(&m.skippedLarge, 1)
		// Any entry already under this key is now stale, and keeping it would
		// serve an older version than the one just returned to the caller.
		m.Invalidate(key)
		return
	}

	if m.budget != nil && !m.budget.Reserve(size) {
		// Nothing anywhere in the process would give up enough room. Serve the
		// caller from what we fetched rather than breaching the ceiling.
		m.Invalidate(key)
		return
	}

	m.mu.Lock()
	var replaced int64
	if el, found := m.items[key]; found {
		item := el.Value.(*Item)
		replaced = item.size
		item.Object = obj
		item.FetchedAt = time.Now()
		item.size = size
		m.lru.MoveToFront(el)
		m.bytes += size - replaced
	} else {
		m.items[key] = m.lru.PushFront(&Item{Object: obj, FetchedAt: time.Now(), key: key, size: size})
		m.bytes += size
	}
	if m.bytes < 0 {
		m.bytes = 0
	}
	m.mu.Unlock()

	// Released outside m.mu, per the lock ordering.
	if replaced > 0 && m.budget != nil {
		m.budget.Release(replaced)
	}
}

// EvictOldest implements Evictor. It takes m.mu itself, so it must only ever be
// called by a caller holding no Manager mutex - in practice the Budget, which
// holds only its own.
//
// Eviction is CLOCK (second chance) rather than strict LRU: an entry whose
// reference bit is set has been read since the last sweep, so it is spared once
// and its bit cleared. This is what lets the hit path stay on the read lock.
func (m *Manager) EvictOldest() (freed int64, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Bounded by the list length: each spared entry has its bit cleared and
	// moves to the front, so a cache whose entries are all referenced still
	// yields a victim within one pass instead of spinning.
	for i, n := 0, m.lru.Len(); i < n; i++ {
		el := m.lru.Back()
		if el == nil {
			return 0, false
		}
		item := el.Value.(*Item)
		if item.ref.Swap(false) {
			m.lru.MoveToFront(el)
			continue
		}
		return m.removeElemLocked(el), true
	}

	// Every entry was referenced during this sweep. Take the one that has gone
	// longest without an eviction pass anyway, so Reserve always makes progress.
	if el := m.lru.Back(); el != nil {
		return m.removeElemLocked(el), true
	}

	return 0, false
}

// removeElemLocked unlinks el, accounts for its bytes and counts the eviction.
// Caller holds m.mu and releases the returned bytes to the budget outside it.
func (m *Manager) removeElemLocked(el *list.Element) int64 {
	item := el.Value.(*Item)
	freed := item.size
	m.lru.Remove(el)
	delete(m.items, item.key)
	m.bytes -= freed
	if m.bytes < 0 {
		m.bytes = 0
	}
	atomic.AddUint64(&m.evictions, 1)

	return freed
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

	m.admit(key, obj)

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
	freed := m.dropLocked(key)
	m.mu.Unlock()

	if freed > 0 && m.budget != nil {
		m.budget.Release(freed)
	}
}

// PurgeIf drops every entry whose key satisfies pred and returns how many were
// dropped. Used by the cluster ownership purge, where the set of keys to drop
// is defined by the hash ring rather than by individual invalidations.
func (m *Manager) PurgeIf(pred func(key string) bool) int {
	if pred == nil {
		return 0
	}

	m.mu.Lock()
	var (
		freed   int64
		dropped int
		next    *list.Element
	)
	for el := m.lru.Front(); el != nil; el = next {
		next = el.Next()
		item := el.Value.(*Item)
		if !pred(item.key) {
			continue
		}
		size := item.size
		m.lru.Remove(el)
		delete(m.items, item.key)
		m.bytes -= size
		freed += size
		dropped++
	}
	if m.bytes < 0 {
		m.bytes = 0
	}
	m.mu.Unlock()

	atomic.AddUint64(&m.purged, uint64(dropped))

	// Released outside m.mu, per the lock ordering.
	if freed > 0 && m.budget != nil {
		m.budget.Release(freed)
	}

	return dropped
}

// dropLocked removes key and returns the bytes it occupied. Caller holds m.mu
// and is responsible for releasing the returned bytes to the budget afterwards,
// outside the lock.
func (m *Manager) dropLocked(key string) int64 {
	el, found := m.items[key]
	if !found {
		return 0
	}

	item := el.Value.(*Item)
	freed := item.size
	m.lru.Remove(el)
	delete(m.items, key)
	m.bytes -= freed
	if m.bytes < 0 {
		m.bytes = 0
	}

	return freed
}

// Stats returns cache hits, misses, and singleflight coalesced hit metrics.
func (m *Manager) Stats() (hits, misses, sfHits uint64) {
	return atomic.LoadUint64(&m.hits), atomic.LoadUint64(&m.misses), atomic.LoadUint64(&m.sfHits)
}

// EvictionStats returns budget evictions, objects skipped for exceeding
// MaxObjectSize, and entries dropped by ownership purges. Kept separate from
// Stats so the existing three-value signature stays source compatible.
func (m *Manager) EvictionStats() (evictions, skippedLarge, purged uint64) {
	return atomic.LoadUint64(&m.evictions),
		atomic.LoadUint64(&m.skippedLarge),
		atomic.LoadUint64(&m.purged)
}

// Budget returns the byte accountant this Manager reserves against, or nil when
// it is unbounded.
func (m *Manager) Budget() *Budget {
	return m.budget
}

// GetCacheSize returns the number of items and approximate bytes in cache. O(1):
// the byte total is maintained on every insert, overwrite, eviction and purge
// because this is called on every metrics scrape.
func (m *Manager) GetCacheSize() (items int, bytes int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.items), m.bytes
}
