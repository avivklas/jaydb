package cache

import (
	"container/list"
	"context"
	"hash/fnv"
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

// shardCount is the number of cache item shards. Must be a power of two so the
// mask-based shard selection works. 64 shards keep per-shard contention low
// even under hundreds of concurrent goroutines while adding negligible memory
// overhead.
const shardCount = 64

// cacheShard holds a subset of cached items and their LRU list behind its own
// mutex so concurrent reads and writes to different keys land on different
// shards and never contend with each other.
type cacheShard struct {
	mu    sync.RWMutex
	items map[string]*list.Element
	lru   *list.List
	bytes int64
}

func (s *cacheShard) dropLocked(key string) int64 {
	el, found := s.items[key]
	if !found {
		return 0
	}
	return s.removeElemLocked(el)
}

func (s *cacheShard) removeElemLocked(el *list.Element) int64 {
	item := el.Value.(*Item)
	freed := item.size
	s.lru.Remove(el)
	delete(s.items, item.key)
	s.bytes -= freed
	if s.bytes < 0 {
		s.bytes = 0
	}
	return freed
}

// Manager coordinates in-memory caching, key-level locking, and singleflight request coalescing.
type Manager struct {
	driver storage.Driver

	// shards partitions cached items across shardCount independent locks so
	// that concurrent cache operations on different keys never contend.
	shards [shardCount]cacheShard

	// sfCalls coalesces concurrent cold-storage fetches for the same key.
	// Using sync.Map instead of a guarded map means registering a singleflight
	// for key A does not block a singleflight for key B.
	sfCalls sync.Map // map[string]*singleflightCall

	keyLocks sync.Map // map[string]*sync.Mutex

	ttl           time.Duration
	maxObjectSize int64

	// budget is shared with the other Managers in this process. NEVER call a
	// method on it while holding any shard mutex: Budget.Reserve calls EvictOldest,
	// which takes shard mutexes itself.
	budget *Budget

	evictCursor atomic.Uint32

	// Metrics counters
	hits         uint64
	misses       uint64
	sfHits       uint64
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
		ttl:           ttl,
		maxObjectSize: cfg.MaxObjectSize,
		budget:        cfg.Budget,
	}

	for i := range m.shards {
		m.shards[i].items = make(map[string]*list.Element)
		m.shards[i].lru = list.New()
	}

	if cfg.Budget != nil {
		cfg.Budget.Register(m)
	}

	return m
}

func (m *Manager) contains(key string) bool {
	shard := m.shardFor(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	_, found := shard.items[key]
	return found
}

// Close unregisters the Manager from its Budget if registered.
func (m *Manager) Close() {
	if m.budget != nil {
		m.budget.Unregister(m)
	}
}

// SetTTL sets the cache expiration duration.
func (m *Manager) SetTTL(d time.Duration) {
	if d <= 0 {
		d = defaultTTL
	}
	m.ttl = d
}

// shardFor returns the cache shard responsible for the given key.
func (m *Manager) shardFor(key string) *cacheShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &m.shards[h.Sum32()&(shardCount-1)]
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
	ttl := m.ttl
	shard := m.shardFor(key)

	// 1. Cache lookup under shard read lock. A hit records recency by setting
	// Item.ref atomically — no write lock needed.
	shard.mu.RLock()
	if el, found := shard.items[key]; found {
		item := el.Value.(*Item)
		if ttl > 0 && time.Since(item.FetchedAt) < ttl {
			obj := item.Object
			item.ref.Store(true)
			shard.mu.RUnlock()
			atomic.AddUint64(&m.hits, 1)
			return obj, nil
		}
	}
	shard.mu.RUnlock()

	atomic.AddUint64(&m.misses, 1)

	// 2. Coalesce cold-storage reads via per-key singleflight (lock-free via sync.Map).
	call := &singleflightCall{done: make(chan struct{})}
	actual, loaded := m.sfCalls.LoadOrStore(key, call)
	if loaded {
		atomic.AddUint64(&m.sfHits, 1)
		return awaitCall(ctx, actual.(*singleflightCall))
	}

	defer func() {
		m.sfCalls.Delete(key)
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

func awaitCall(ctx context.Context, call *singleflightCall) (*storage.Object, error) {
	select {
	case <-call.done:
		return call.val, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *Manager) admit(key string, obj *storage.Object) {
	if obj == nil {
		return
	}

	size := objectSize(obj)
	if m.maxObjectSize > 0 && size > m.maxObjectSize {
		atomic.AddUint64(&m.skippedLarge, 1)
		m.Invalidate(key)
		return
	}

	if m.budget != nil && !m.budget.Reserve(size) {
		m.Invalidate(key)
		return
	}

	shard := m.shardFor(key)
	shard.mu.Lock()
	var replaced int64
	if el, found := shard.items[key]; found {
		item := el.Value.(*Item)
		replaced = item.size
		item.Object = obj
		item.FetchedAt = time.Now()
		item.size = size
		shard.lru.MoveToFront(el)
		shard.bytes += size - replaced
	} else {
		shard.items[key] = shard.lru.PushFront(&Item{Object: obj, FetchedAt: time.Now(), key: key, size: size})
		shard.bytes += size
	}
	if shard.bytes < 0 {
		shard.bytes = 0
	}
	shard.mu.Unlock()

	if replaced > 0 && m.budget != nil {
		m.budget.Release(replaced)
	}
}

// EvictOldest implements Evictor across shards using CLOCK (second chance) and recency.
func (m *Manager) EvictOldest() (freed int64, ok bool) {
	var (
		bestShard       *cacheShard
		bestElem        *list.Element
		oldestTime      time.Time
		bestUnrefShard  *cacheShard
		bestUnrefElem   *list.Element
		oldestUnrefTime time.Time
	)

	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		if el := shard.lru.Back(); el != nil {
			item := el.Value.(*Item)
			if oldestTime.IsZero() || item.FetchedAt.Before(oldestTime) {
				oldestTime = item.FetchedAt
				bestShard = shard
				bestElem = el
			}
			if !item.ref.Load() {
				if oldestUnrefTime.IsZero() || item.FetchedAt.Before(oldestUnrefTime) {
					oldestUnrefTime = item.FetchedAt
					bestUnrefShard = shard
					bestUnrefElem = el
				}
			}
		}
		shard.mu.RUnlock()
	}

	targetShard := bestUnrefShard
	targetElem := bestUnrefElem
	if targetShard == nil {
		targetShard = bestShard
		targetElem = bestElem
	}

	if targetShard == nil || targetElem == nil {
		return 0, false
	}

	targetShard.mu.Lock()
	item := targetElem.Value.(*Item)
	if el, found := targetShard.items[item.key]; found && el == targetElem {
		freed = targetShard.removeElemLocked(targetElem)
		targetShard.mu.Unlock()
		atomic.AddUint64(&m.evictions, 1)
		return freed, true
	}
	targetShard.mu.Unlock()

	// If a race occurred, retry candidate selection
	return 0, false
}

// Put writes an object to cold storage under key-level mutex and populates the cache.
func (m *Manager) Put(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	keyLock := m.getKeyMutex(key)
	keyLock.Lock()
	defer func() {
		keyLock.Unlock()
		m.keyLocks.Delete(key) // allow GC; next write will re-create
	}()

	obj, err := m.driver.Put(ctx, key, value, expectedETag)
	if err != nil {
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
	defer func() {
		keyLock.Unlock()
		m.keyLocks.Delete(key) // allow GC; next write will re-create
	}()

	err := m.driver.Delete(ctx, key, expectedETag)
	m.Invalidate(key)
	return err
}

// Invalidate removes a key from the in-memory cache.
func (m *Manager) Invalidate(key string) {
	shard := m.shardFor(key)
	shard.mu.Lock()
	freed := shard.dropLocked(key)
	shard.mu.Unlock()

	if freed > 0 && m.budget != nil {
		m.budget.Release(freed)
	}
}

// PurgeIf drops every entry whose key satisfies pred and returns how many were dropped.
func (m *Manager) PurgeIf(pred func(key string) bool) int {
	if pred == nil {
		return 0
	}

	var (
		freed   int64
		dropped int
	)

	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.Lock()
		var next *list.Element
		for el := shard.lru.Front(); el != nil; el = next {
			next = el.Next()
			item := el.Value.(*Item)
			if !pred(item.key) {
				continue
			}
			size := item.size
			shard.lru.Remove(el)
			delete(shard.items, item.key)
			shard.bytes -= size
			freed += size
			dropped++
		}
		if shard.bytes < 0 {
			shard.bytes = 0
		}
		shard.mu.Unlock()
	}

	atomic.AddUint64(&m.purged, uint64(dropped))

	if freed > 0 && m.budget != nil {
		m.budget.Release(freed)
	}

	return dropped
}

// Stats returns cache hits, misses, and singleflight coalesced hit metrics.
func (m *Manager) Stats() (hits, misses, sfHits uint64) {
	return atomic.LoadUint64(&m.hits), atomic.LoadUint64(&m.misses), atomic.LoadUint64(&m.sfHits)
}

// EvictionStats returns budget evictions, objects skipped for exceeding
// MaxObjectSize, and entries dropped by ownership purges.
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

// GetCacheSize returns the total item count and byte size across all shards.
func (m *Manager) GetCacheSize() (items int, bytes int64) {
	for i := range m.shards {
		shard := &m.shards[i]
		shard.mu.RLock()
		items += len(shard.items)
		bytes += shard.bytes
		shard.mu.RUnlock()
	}
	return items, bytes
}
