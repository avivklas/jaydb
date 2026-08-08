package cache

import "sync"

// Evictor is the Manager side of the Budget contract: something that can hand
// memory back one entry at a time when the process ceiling is reached.
type Evictor interface {
	// EvictOldest drops the single least-recently-used entry and returns the
	// bytes freed. ok==false means the Evictor has nothing left to give.
	//
	// The Budget adjusts its own accounting from the returned value, so an
	// implementation must NOT call Budget.Release for the entry it just
	// dropped - the Budget's mutex is already held when this runs.
	EvictOldest() (freed int64, ok bool)
}

// Budget is a byte accountant shared by several cache Managers so a process
// hosting many namespaces honours one global memory ceiling. Without it each
// db.Open gets an independent unbounded map, and a 512 MB task hosting a few
// dozen namespaces has no ceiling at all.
//
// LOCK ORDERING - do not violate this or the process deadlocks. Budget.mu is
// held for the whole Reserve-with-eviction sequence, and Evictor.EvictOldest
// takes the target Manager's own mutex while it runs. So Budget.mu is always
// acquired BEFORE any Manager mutex and Manager mutexes are leaves. A Manager
// must therefore never call any Budget method while holding its own mutex; if
// it did, the two lock orders would cross.
type Budget struct {
	mu       sync.Mutex
	limit    int64
	used     int64
	evictors []Evictor

	// cursor rotates the eviction victim. Always starting from evictors[0]
	// would make the first-registered namespace absorb every eviction in the
	// process while later ones grow unchecked.
	cursor int
}

// NewBudget returns a byte accountant capped at limitBytes. A limit <= 0 means
// unbounded: reservations always succeed, but usage is still tracked so the
// exported gauge stays meaningful.
func NewBudget(limitBytes int64) *Budget {
	return &Budget{limit: limitBytes}
}

// Register enrolls a Manager as an eviction source. Registration order only
// affects where the round-robin cursor starts.
func (b *Budget) Register(e Evictor) {
	if e == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.evictors = append(b.evictors, e)
}

// Reserve accounts for n bytes about to be cached, evicting across registered
// Managers if that is what it takes to fit. It reports false when even a fully
// drained cache could not make room, in which case the caller must not cache
// the object.
//
// Callers must not hold their own mutex here - see the lock-ordering note on
// Budget.
func (b *Budget) Reserve(n int64) bool {
	if n < 0 {
		n = 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.limit <= 0 || b.used+n <= b.limit {
		b.used += n
		return true
	}

	// An object bigger than the entire ceiling can never fit, so refuse it
	// BEFORE evicting anything. Without this check the loop below drains every
	// registered Manager down to empty - discarding every namespace's working
	// set across the whole process - and then still fails to admit the object,
	// so the next request for that key repeats the whole thing. A per-object
	// cap normally keeps such objects away from here, but MaxObjectSize and
	// Budget are independent options and a cap of 0 is a documented no-cap.
	if n > b.limit {
		return false
	}

	// Ask Evictors round-robin. exhausted counts consecutive Evictors with
	// nothing left; once every one of them has refused in a row there is no
	// more memory to reclaim anywhere and the loop must stop.
	exhausted := 0
	for len(b.evictors) > 0 && exhausted < len(b.evictors) {
		e := b.evictors[b.cursor]
		b.cursor = (b.cursor + 1) % len(b.evictors)

		freed, ok := e.EvictOldest()
		if !ok {
			exhausted++
			continue
		}
		exhausted = 0

		b.used -= freed
		if b.used < 0 {
			b.used = 0
		}
		if b.used+n <= b.limit {
			b.used += n
			return true
		}
	}

	return false
}

// Release returns n previously reserved bytes to the budget.
//
// Callers must not hold their own mutex here - see the lock-ordering note on
// Budget.
func (b *Budget) Release(n int64) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}
}

// Used returns the bytes currently accounted for across all registered Managers.
func (b *Budget) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// Limit returns the configured ceiling in bytes; <= 0 means unbounded.
func (b *Budget) Limit() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limit
}
