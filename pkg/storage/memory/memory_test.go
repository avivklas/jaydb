package memory

import (
	"context"
	"testing"

	"github.com/avivklas/jaydb/pkg/storage"
)

func TestMemoryDriver_CRUD_CAS(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver()

	// 1. Get non-existent
	_, err := driver.Get(ctx, "k1")
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// 2. Create object (If-None-Match: *)
	obj1, err := driver.Put(ctx, "k1", []byte("val1"), storage.MatchAnyETag)
	if err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}
	if obj1.ETag == "" {
		t.Fatalf("expected non-empty ETag")
	}

	// 3. Create object again with MatchAnyETag should fail
	_, err = driver.Put(ctx, "k1", []byte("val2"), storage.MatchAnyETag)
	if err != storage.ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	// 4. Update with wrong ETag
	_, err = driver.Put(ctx, "k1", []byte("val2"), `"wrongetag"`)
	if err != storage.ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// 5. Update with correct ETag
	obj2, err := driver.Put(ctx, "k1", []byte("val2"), obj1.ETag)
	if err != nil {
		t.Fatalf("unexpected update error: %v", err)
	}
	if obj2.ETag == obj1.ETag {
		t.Fatalf("expected ETag to change after update")
	}

	// 6. Delete with wrong ETag
	err = driver.Delete(ctx, "k1", obj1.ETag)
	if err != storage.ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch on delete, got %v", err)
	}

	// 7. Delete with correct ETag
	err = driver.Delete(ctx, "k1", obj2.ETag)
	if err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}

	// 8. Confirm deleted
	_, err = driver.Get(ctx, "k1")
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestMemoryDriver_List(t *testing.T) {
	ctx := context.Background()
	driver := NewDriver()

	keys := []string{
		"users/1/profile",
		"users/1/posts/p1",
		"users/1/posts/p2",
		"users/2/profile",
	}

	for _, k := range keys {
		if _, err := driver.Put(ctx, k, []byte("data"), ""); err != nil {
			t.Fatalf("unexpected error setting key %s: %v", k, err)
		}
	}

	// List prefix "users/1/"
	res, _, err := driver.List(ctx, "users/1/", storage.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected list error: %v", err)
	}

	if len(res) != 3 {
		t.Fatalf("expected 3 items for users/1/, got %d", len(res))
	}
}
