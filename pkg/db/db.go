package db

import (
	"context"
	"fmt"
	"time"

	"github.com/avivklas/jaydb/pkg/cache"
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
	Cache() *cache.Manager
	ShardingDepth() int
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

	return &database{
		opts:         opts,
		cacheMgr:     cacheMgr,
		codec:        opts.Codec,
		storageDrive: opts.Storage,
	}, nil
}

func (d *database) Get(ctx context.Context, key string, dest any) (*Meta, error) {
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
	return d.cacheMgr.Delete(ctx, key, do.ExpectedETag)
}

func (d *database) List(ctx context.Context, prefix string, limit int) ([]*Item, error) {
	metas, _, err := d.storageDrive.List(ctx, prefix, storage.ListOptions{Limit: limit})
	if err != nil {
		return nil, err
	}

	var items []*Item
	for _, m := range metas {
		items = append(items, &Item{
			Meta: Meta{
				Key:     m.Key,
				ETag:    m.ETag,
				ModTime: m.ModTime,
			},
		})
	}
	return items, nil
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
	return d.storageDrive.Close()
}
