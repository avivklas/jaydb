package fs

import (
	"context"
	"os"
	"testing"

	"github.com/avivklas/jaydb/pkg/storage"
)

func TestFSDriver_CRUD_CAS(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "s3db_fs_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	driver, err := NewDriver(tmpDir)
	if err != nil {
		t.Fatalf("failed to create fs driver: %v", err)
	}

	// 1. Get non-existent
	_, err = driver.Get(ctx, "users/1/profile")
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// 2. Put create
	obj1, err := driver.Put(ctx, "users/1/profile", []byte("alice"), storage.MatchAnyETag)
	if err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}

	// 3. Put with wrong etag
	_, err = driver.Put(ctx, "users/1/profile", []byte("bob"), `"wrong"`)
	if err != storage.ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// 4. Put with correct etag
	obj2, err := driver.Put(ctx, "users/1/profile", []byte("bob"), obj1.ETag)
	if err != nil {
		t.Fatalf("unexpected update error: %v", err)
	}

	// 5. Read back
	readObj, err := driver.Get(ctx, "users/1/profile")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if string(readObj.Value) != "bob" {
		t.Fatalf("expected 'bob', got '%s'", readObj.Value)
	}
	if readObj.ETag != obj2.ETag {
		t.Fatalf("expected etag %s, got %s", obj2.ETag, readObj.ETag)
	}

	// 6. Delete
	if err := driver.Delete(ctx, "users/1/profile", obj2.ETag); err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}
}
