package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/avivklas/jaydb/pkg/cache"
	"github.com/avivklas/jaydb/pkg/cluster"
	"github.com/avivklas/jaydb/pkg/encoding"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// Meta contains metadata for a database document.
type Meta struct {
	Key     string    `json:"key"`
	ETag    string    `json:"etag"`
	ModTime time.Time `json:"mod_time"`

	// Size is the stored byte size of the document. Populated by listings,
	// where the storage layer reports it without transferring the payload, so
	// callers can measure a keyspace without reading it.
	Size int64 `json:"size,omitempty"`
}

// Item represents a listed key along with optional unmarshaled content or raw bytes.
type Item struct {
	Meta  Meta   `json:"meta"`
	Value []byte `json:"value,omitempty"`
}

// Options configures the embedded database instance.
type Options struct {
	Storage       storage.Driver
	Codec         encoding.Codec
	ShardingDepth int
	Ring          *sharding.Ring
	ClusterNode   *cluster.Node

	// Namespace identifies this database within a clustered process that hosts
	// several of them. It is required for inter-query routing to be correct
	// when more than one DB shares a ClusterNode: the wire request carries the
	// namespace so the owner node can pick the matching local DB. Leave empty
	// for a single embedded database.
	Namespace string
}

// PutOptions specifies options for write operations.
type PutOptions struct {
	ExpectedETag string
}

// PutOption is a functional option for Put.
type PutOption func(*PutOptions)

// WithExpectedETag specifies optimistic concurrency control (CAS) tag.
func WithExpectedETag(etag string) PutOption {
	return func(o *PutOptions) {
		o.ExpectedETag = etag
	}
}

// CreateOnly specifies write should fail if key already exists (If-None-Match: *).
func CreateOnly() PutOption {
	return func(o *PutOptions) {
		o.ExpectedETag = storage.MatchAnyETag
	}
}

// DeleteOptions specifies options for delete operations.
type DeleteOptions struct {
	ExpectedETag string
}

// DeleteOption is a functional option for Delete.
type DeleteOption func(*DeleteOptions)

func WithDeleteExpectedETag(etag string) DeleteOption {
	return func(o *DeleteOptions) {
		o.ExpectedETag = etag
	}
}

// DB defines the high-level embedded document database interface.
type DB interface {
	Get(ctx context.Context, key string, dest any) (*Meta, error)
	Put(ctx context.Context, key string, doc any, opts ...PutOption) (*Meta, error)
	Delete(ctx context.Context, key string, opts ...DeleteOption) error
	List(ctx context.Context, prefix string, limit int) ([]*Item, error)
	// ListPage returns one page of keys plus a cursor for the next page. An
	// empty returned cursor means the keyspace is exhausted.
	ListPage(ctx context.Context, prefix string, opts ListPageOptions) (items []*Item, next string, err error)
	Cache() *cache.Manager
	ShardingDepth() int
	GetRaw(ctx context.Context, key string) (*storage.Object, error)
	PutRaw(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error)
	DeleteRaw(ctx context.Context, key string, expectedETag string) error
	Close() error
}

type database struct {
	opts         Options
	cacheMgr     *cache.Manager
	codec        encoding.Codec
	storageDrive storage.Driver
}

// Open initializes and returns an embedded document database.
func Open(opts Options) (DB, error) {
	if opts.Storage == nil {
		opts.Storage = memory.NewDriver()
	}
	if opts.Codec == nil {
		opts.Codec = encoding.NewJSONCodec()
	}
	if opts.ShardingDepth <= 0 {
		opts.ShardingDepth = 2
	}

	cacheMgr := cache.NewManager(opts.Storage)

	d := &database{
		opts:         opts,
		cacheMgr:     cacheMgr,
		codec:        opts.Codec,
		storageDrive: opts.Storage,
	}

	// Register as the owner-side handler for this namespace. Without this the
	// cluster node has nothing to serve forwarded requests with, and peers
	// received an empty response that looked exactly like a missing document.
	if opts.ClusterNode != nil {
		opts.ClusterNode.RegisterHandler(opts.Namespace, d)
	}

	return d, nil
}

func (d *database) Get(ctx context.Context, key string, dest any) (*Meta, error) {
	// Inter-query routing check
	if d.opts.Ring != nil && d.opts.ClusterNode != nil {
		targetNode := d.opts.Ring.GetNode(key)
		selfAddr := d.opts.ClusterNode.SelfQuicAddr()
		if targetNode != "" && targetNode != selfAddr {
			resp, err := d.opts.ClusterNode.ExecuteInterQuery(ctx, targetNode, cluster.InterQueryReq{
				Namespace: d.opts.Namespace,
				Op:        cluster.OpGet,
				Key:       key,
			})
			if err != nil {
				return nil, fmt.Errorf("inter-query error: %w", err)
			}
			if resp.Err != "" {
				return nil, mapErrorString(resp.Err)
			}
			// A genuinely absent document always arrives as resp.Err ==
			// storage.ErrNotFound. So "no object and no error" means the peer
			// could not answer - reporting that as ErrNotFound is what made a
			// broken mesh look like data loss.
			if resp.Object == nil {
				return nil, fmt.Errorf("inter-query to %s: %w", targetNode, cluster.ErrIncompleteResponse)
			}
			if dest != nil {
				if err := d.codec.Unmarshal(resp.Object.Value, dest); err != nil {
					return nil, fmt.Errorf("db decode error: %w", err)
				}
			}
			return &Meta{
				Key:     resp.Object.Key,
				ETag:    resp.Object.ETag,
				ModTime: resp.Object.ModTime,
			}, nil
		}
	}

	obj, err := d.cacheMgr.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if dest != nil {
		if err := d.codec.Unmarshal(obj.Value, dest); err != nil {
			return nil, fmt.Errorf("db decode error: %w", err)
		}
	}

	return &Meta{
		Key:     obj.Key,
		ETag:    obj.ETag,
		ModTime: obj.ModTime,
	}, nil
}

func (d *database) Put(ctx context.Context, key string, doc any, opts ...PutOption) (*Meta, error) {
	var po PutOptions
	for _, opt := range opts {
		opt(&po)
	}

	data, err := d.codec.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("db encode error: %w", err)
	}

	// Inter-query routing check
	if d.opts.Ring != nil && d.opts.ClusterNode != nil {
		targetNode := d.opts.Ring.GetNode(key)
		selfAddr := d.opts.ClusterNode.SelfQuicAddr()
		if targetNode != "" && targetNode != selfAddr {
			resp, err := d.opts.ClusterNode.ExecuteInterQuery(ctx, targetNode, cluster.InterQueryReq{
				Namespace:    d.opts.Namespace,
				Op:           cluster.OpPut,
				Key:          key,
				Value:        data,
				ExpectedETag: po.ExpectedETag,
			})
			if err != nil {
				return nil, fmt.Errorf("inter-query error: %w", err)
			}
			if resp.Err != "" {
				return nil, mapErrorString(resp.Err)
			}
			// Guard the dereference: a peer that answered with neither an object
			// nor an error would otherwise panic the process here.
			if resp.Object == nil {
				return nil, fmt.Errorf("inter-query to %s: %w", targetNode, cluster.ErrIncompleteResponse)
			}
			return &Meta{
				Key:     resp.Object.Key,
				ETag:    resp.Object.ETag,
				ModTime: resp.Object.ModTime,
			}, nil
		}
	}

	obj, err := d.cacheMgr.Put(ctx, key, data, po.ExpectedETag)
	if err != nil {
		return nil, err
	}

	return &Meta{
		Key:     obj.Key,
		ETag:    obj.ETag,
		ModTime: obj.ModTime,
	}, nil
}

func (d *database) Delete(ctx context.Context, key string, opts ...DeleteOption) error {
	var do DeleteOptions
	for _, opt := range opts {
		opt(&do)
	}

	// Inter-query routing check
	if d.opts.Ring != nil && d.opts.ClusterNode != nil {
		targetNode := d.opts.Ring.GetNode(key)
		selfAddr := d.opts.ClusterNode.SelfQuicAddr()
		if targetNode != "" && targetNode != selfAddr {
			resp, err := d.opts.ClusterNode.ExecuteInterQuery(ctx, targetNode, cluster.InterQueryReq{
				Namespace:    d.opts.Namespace,
				Op:           cluster.OpDelete,
				Key:          key,
				ExpectedETag: do.ExpectedETag,
			})
			if err != nil {
				return fmt.Errorf("inter-query error: %w", err)
			}
			if resp.Err != "" {
				return mapErrorString(resp.Err)
			}
			return nil
		}
	}

	return d.cacheMgr.Delete(ctx, key, do.ExpectedETag)
}

func (d *database) GetRaw(ctx context.Context, key string) (*storage.Object, error) {
	return d.cacheMgr.Get(ctx, key)
}

func (d *database) PutRaw(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	return d.cacheMgr.Put(ctx, key, value, expectedETag)
}

func (d *database) DeleteRaw(ctx context.Context, key string, expectedETag string) error {
	return d.cacheMgr.Delete(ctx, key, expectedETag)
}

// listPageSize is the number of keys requested from storage per underlying
// listing call. It matches the S3 maximum, which is the tightest limit among the
// drivers, so one page here is always exactly one storage request.
const listPageSize = 1000

// ListPageOptions configures a single page of a listing.
type ListPageOptions struct {
	// Limit caps the number of keys in this page. Zero or negative means the
	// driver's natural page size (listPageSize).
	Limit int

	// Cursor resumes after a previous page. Empty starts from the beginning.
	Cursor string
}

// List returns up to limit keys under prefix.
//
// The limit is honored across page boundaries. This used to issue exactly one
// storage listing and pass the limit straight through, discarding the cursor the
// driver returned - so on S3, which caps a response at 1000 keys no matter what
// MaxKeys asks for, List(prefix, 5000) silently returned 1000 keys and no
// indication that more existed. Every caller inherited that truncation: a
// listing endpoint under-reported, and anything counting a keyspace stopped
// counting at 1000.
//
// A limit of zero or less means "every matching key", which is unbounded by
// definition - prefer ListPage when the keyspace may be large.
func (d *database) List(ctx context.Context, prefix string, limit int) ([]*Item, error) {
	var (
		items  []*Item
		cursor string
	)

	for {
		pageLimit := listPageSize
		if limit > 0 {
			if remaining := limit - len(items); remaining < pageLimit {
				pageLimit = remaining
			}
		}

		page, next, err := d.ListPage(ctx, prefix, ListPageOptions{
			Limit:  pageLimit,
			Cursor: cursor,
		})
		if err != nil {
			return nil, err
		}
		items = append(items, page...)

		if limit > 0 && len(items) >= limit {
			return items[:limit], nil
		}
		// A cursor with no keys behind it means no progress: stop rather than
		// spin, so a driver that reports a stale cursor cannot hang the caller.
		if next == "" || len(page) == 0 {
			return items, nil
		}
		cursor = next
	}
}

// ListPage returns one page of keys under prefix, plus the cursor to resume from.
func (d *database) ListPage(ctx context.Context, prefix string, opts ListPageOptions) ([]*Item, string, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = listPageSize
	}

	metas, next, err := d.storageDrive.List(ctx, prefix, storage.ListOptions{
		Limit:  limit,
		Cursor: opts.Cursor,
	})
	if err != nil {
		return nil, "", err
	}

	items := make([]*Item, 0, len(metas))
	for _, m := range metas {
		if m == nil {
			continue
		}
		items = append(items, &Item{
			Meta: Meta{
				Key:     m.Key,
				ETag:    m.ETag,
				ModTime: m.ModTime,
				Size:    m.Size,
			},
		})
	}
	return items, next, nil
}

func (d *database) Cache() *cache.Manager {
	return d.cacheMgr
}

func (d *database) ShardingDepth() int {
	return d.opts.ShardingDepth
}

func (d *database) PartitionKey(key string) string {
	return sharding.PartitionKey(key, d.opts.ShardingDepth)
}

func (d *database) Close() error {
	if d.opts.ClusterNode != nil {
		// Stop serving forwarded requests for this namespace before tearing the
		// storage driver down, so peers get an explicit error instead of hitting
		// a closed driver.
		d.opts.ClusterNode.UnregisterHandler(d.opts.Namespace)
		_ = d.opts.ClusterNode.Close()
	}
	return d.storageDrive.Close()
}

func mapErrorString(errStr string) error {
	switch errStr {
	case storage.ErrNotFound.Error():
		return storage.ErrNotFound
	case storage.ErrAlreadyExists.Error():
		return storage.ErrAlreadyExists
	case storage.ErrVersionMismatch.Error():
		return storage.ErrVersionMismatch
	default:
		return errors.New(errStr)
	}
}
