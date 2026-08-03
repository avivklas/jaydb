package storage

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound is returned when a requested key does not exist.
	ErrNotFound = errors.New("storage: object not found")

	// ErrVersionMismatch is returned when an optimistic locking (CAS) check fails.
	ErrVersionMismatch = errors.New("storage: CAS version mismatch (412 Precondition Failed)")

	// ErrAlreadyExists is returned when writing with create-only semantics (If-None-Match: *) and the key exists.
	ErrAlreadyExists = errors.New("storage: object already exists")
)

// MatchAnyETag can be passed as expectedETag to create an object only if it does not exist (If-None-Match: *).
const MatchAnyETag = "*"

// Object represents a stored document payload along with its cold-storage metadata.
type Object struct {
	Key      string
	Value    []byte
	ETag     string
	ModTime  time.Time
	Metadata map[string]string
}

// KeyMeta provides metadata for an object without returning the full payload.
type KeyMeta struct {
	Key     string
	Size    int64
	ETag    string
	ModTime time.Time
}

// ListOptions specifies parameters for tree prefix queries.
type ListOptions struct {
	Limit     int
	Cursor    string
	Delimiter string
}

// Driver defines the cold storage engine interface (S3, FS, Memory).
type Driver interface {
	// Get fetches an object by key. Returns ErrNotFound if it does not exist.
	Get(ctx context.Context, key string) (*Object, error)

	// Put stores an object with optional Optimistic Concurrency Control (CAS).
	// If expectedETag is non-empty, the update succeeds only if the current ETag matches expectedETag.
	// If expectedETag is "*", write succeeds only if object does NOT already exist.
	Put(ctx context.Context, key string, value []byte, expectedETag string) (*Object, error)

	// Delete removes an object by key with optional CAS check.
	Delete(ctx context.Context, key string, expectedETag string) error

	// List queries objects matching prefix. Returns list of KeyMeta and next cursor.
	List(ctx context.Context, prefix string, opts ListOptions) ([]*KeyMeta, string, error)

	// Close cleans up driver connections or background tasks.
	Close() error
}
