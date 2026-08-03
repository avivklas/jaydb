package db

import (
	"context"
	"testing"

	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

type User struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

func TestDB_Embedded_Operations(t *testing.T) {
	ctx := context.Background()
	database, err := Open(Options{
		Storage:       memory.NewDriver(),
		ShardingDepth: 2,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	// 1. Create User (CreateOnly / If-None-Match: *)
	u1 := User{ID: "u1", Email: "alice@example.com"}
	meta1, err := database.Put(ctx, "users/u1/profile", u1, CreateOnly())
	if err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}
	if meta1.ETag == "" {
		t.Fatalf("expected non-empty etag")
	}

	// 2. Read User
	var readUser User
	readMeta, err := database.Get(ctx, "users/u1/profile", &readUser)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if readUser.Email != "alice@example.com" {
		t.Fatalf("expected email 'alice@example.com', got '%s'", readUser.Email)
	}
	if readMeta.ETag != meta1.ETag {
		t.Fatalf("expected ETag %s, got %s", meta1.ETag, readMeta.ETag)
	}

	// 3. Put with Stale ETag fails with CAS ErrVersionMismatch
	u1Update := User{ID: "u1", Email: "alice-updated@example.com"}
	_, err = database.Put(ctx, "users/u1/profile", u1Update, WithExpectedETag(`"stale-etag"`))
	if err != storage.ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// 4. Put with Valid ETag succeeds
	meta2, err := database.Put(ctx, "users/u1/profile", u1Update, WithExpectedETag(meta1.ETag))
	if err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}
	if meta2.ETag == meta1.ETag {
		t.Fatalf("expected ETag to change after update")
	}

	// 5. Delete with Valid ETag
	if err := database.Delete(ctx, "users/u1/profile", WithDeleteExpectedETag(meta2.ETag)); err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}

	// 6. Read after delete returns ErrNotFound
	_, err = database.Get(ctx, "users/u1/profile", &readUser)
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}
