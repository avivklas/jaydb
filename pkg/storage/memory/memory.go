package memory

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/avivklas/jaydb/pkg/storage"
)

type entry struct {
	value   []byte
	etag    string
	modTime time.Time
}

// Driver is a thread-safe in-memory cold storage implementation.
type Driver struct {
	mu    sync.RWMutex
	data  map[string]*entry
	clock func() time.Time
}

// NewDriver returns a new in-memory storage driver.
func NewDriver() storage.Driver {
	return &Driver{
		data:  make(map[string]*entry),
		clock: time.Now,
	}
}

func generateETag(data []byte, modTime time.Time) string {
	h := sha256.New()
	h.Write(data)
	h.Write([]byte(fmt.Sprintf("%d", modTime.UnixNano())))
	return fmt.Sprintf(`"%x"`, h.Sum(nil)[:12])
}

func (d *Driver) Get(ctx context.Context, key string) (*storage.Object, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	e, ok := d.data[key]
	if !ok {
		return nil, storage.ErrNotFound
	}

	valCopy := append([]byte(nil), e.value...)
	return &storage.Object{
		Key:     key,
		Value:   valCopy,
		ETag:    e.etag,
		ModTime: e.modTime,
	}, nil
}

func (d *Driver) Put(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	existing, exists := d.data[key]

	if expectedETag != "" {
		if expectedETag == storage.MatchAnyETag {
			if exists {
				return nil, storage.ErrAlreadyExists
			}
		} else {
			if !exists {
				return nil, storage.ErrNotFound
			}
			if existing.etag != expectedETag {
				return nil, storage.ErrVersionMismatch
			}
		}
	}

	now := d.clock()
	valCopy := append([]byte(nil), value...)
	newETag := generateETag(valCopy, now)

	newEntry := &entry{
		value:   valCopy,
		etag:    newETag,
		modTime: now,
	}

	d.data[key] = newEntry

	return &storage.Object{
		Key:     key,
		Value:   append([]byte(nil), valCopy...),
		ETag:    newETag,
		ModTime: now,
	}, nil
}

func (d *Driver) Delete(ctx context.Context, key string, expectedETag string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	existing, exists := d.data[key]
	if !exists {
		return storage.ErrNotFound
	}

	if expectedETag != "" && expectedETag != storage.MatchAnyETag {
		if existing.etag != expectedETag {
			return storage.ErrVersionMismatch
		}
	}

	delete(d.data, key)
	return nil
}

func (d *Driver) List(ctx context.Context, prefix string, opts storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var keys []string
	for k := range d.data {
		if strings.HasPrefix(k, prefix) {
			if opts.Cursor != "" && k <= opts.Cursor {
				continue
			}
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	var results []*storage.KeyMeta
	var nextCursor string

	for _, k := range keys {
		if opts.Limit > 0 && len(results) >= opts.Limit {
			break
		}

		rel := strings.TrimPrefix(k, prefix)
		if opts.Delimiter != "" {
			idx := strings.Index(rel, opts.Delimiter)
			if idx != -1 {
				dirKey := prefix + rel[:idx+len(opts.Delimiter)]
				if len(results) > 0 && results[len(results)-1].Key == dirKey {
					continue
				}
				results = append(results, &storage.KeyMeta{
					Key: dirKey,
				})
				nextCursor = k
				continue
			}
		}

		e := d.data[k]
		results = append(results, &storage.KeyMeta{
			Key:     k,
			Size:    int64(len(e.value)),
			ETag:    e.etag,
			ModTime: e.modTime,
		})
		nextCursor = k
	}

	if opts.Limit <= 0 || len(results) < opts.Limit {
		nextCursor = ""
	}

	return results, nextCursor, nil
}

func (d *Driver) Close() error {
	return nil
}
