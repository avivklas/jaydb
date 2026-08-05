package db_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/cache"
	"github.com/avivklas/jaydb/pkg/cluster"
	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/metrics"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// TestEmbeddedMode_FeatureParity verifies that all non-HTTP features work in pure embedded mode.
// This test ensures embedded mode has feature parity with server mode (except HTTP endpoints).
func TestEmbeddedMode_FeatureParity(t *testing.T) {
	ctx := context.Background()

	t.Run("DatabaseOperations", func(t *testing.T) {
		database, err := db.Open(db.Options{
			Storage:       memory.NewDriver(),
			ShardingDepth: 2,
		})
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		type User struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		// Put
		user := User{Name: "Alice", Age: 30}
		meta, err := database.Put(ctx, "users/alice", user)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if meta.ETag == "" {
			t.Error("Expected ETag")
		}

		// Get
		var readUser User
		readMeta, err := database.Get(ctx, "users/alice", &readUser)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if readUser.Name != "Alice" || readUser.Age != 30 {
			t.Errorf("Got %+v, want %+v", readUser, user)
		}
		if readMeta.ETag != meta.ETag {
			t.Error("ETag mismatch")
		}

		// List
		database.Put(ctx, "users/bob", User{Name: "Bob", Age: 25})
		items, err := database.List(ctx, "users/", 10)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(items) != 2 {
			t.Errorf("Got %d items, want 2", len(items))
		}

		// Delete
		err = database.Delete(ctx, "users/bob")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = database.Get(ctx, "users/bob", &readUser)
		if err != storage.ErrNotFound {
			t.Errorf("Expected ErrNotFound after delete, got: %v", err)
		}
	})

	t.Run("CAS_OptimisticLocking", func(t *testing.T) {
		database, err := db.Open(db.Options{
			Storage: memory.NewDriver(),
		})
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		// CreateOnly (If-None-Match: *)
		data := map[string]string{"status": "draft"}
		meta, err := database.Put(ctx, "doc/1", data, db.CreateOnly())
		if err != nil {
			t.Fatalf("CreateOnly failed: %v", err)
		}

		// Duplicate create should fail
		_, err = database.Put(ctx, "doc/1", data, db.CreateOnly())
		if err != storage.ErrAlreadyExists {
			t.Errorf("Expected ErrAlreadyExists, got: %v", err)
		}

		// CAS update (If-Match: etag)
		data["status"] = "published"
		newMeta, err := database.Put(ctx, "doc/1", data, db.WithExpectedETag(meta.ETag))
		if err != nil {
			t.Fatalf("CAS update failed: %v", err)
		}
		if newMeta.ETag == meta.ETag {
			t.Error("ETag should change after update")
		}

		// Stale ETag should fail
		_, err = database.Put(ctx, "doc/1", data, db.WithExpectedETag(meta.ETag))
		if err != storage.ErrVersionMismatch {
			t.Errorf("Expected ErrVersionMismatch, got: %v", err)
		}

		// CAS delete
		err = database.Delete(ctx, "doc/1", db.WithDeleteExpectedETag(newMeta.ETag))
		if err != nil {
			t.Fatalf("CAS delete failed: %v", err)
		}
	})

	t.Run("CacheAndSingleflight", func(t *testing.T) {
		database, err := db.Open(db.Options{
			Storage: memory.NewDriver(),
		})
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		cacheManager := database.Cache()
		if cacheManager == nil {
			t.Fatal("Cache() returned nil")
		}

		// Verify cache has required methods
		hits, misses, sfHits := cacheManager.Stats()
		if hits < 0 || misses < 0 || sfHits < 0 {
			t.Error("Invalid cache stats")
		}

		items, bytes := cacheManager.GetCacheSize()
		if items < 0 || bytes < 0 {
			t.Error("Invalid cache size")
		}

		// Put and read (cache hit)
		data := map[string]string{"key": "value"}
		database.Put(ctx, "test/cache", data)

		initialHits := hits
		var readData map[string]string
		database.Get(ctx, "test/cache", &readData)

		hits, _, _ = cacheManager.Stats()
		if hits <= initialHits {
			t.Error("Expected cache hit count to increase")
		}
	})

	t.Run("PrometheusMetrics", func(t *testing.T) {
		database, err := db.Open(db.Options{
			Storage: memory.NewDriver(),
		})
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		// Create metrics collector (works without HTTP server)
		collector := metrics.NewCollector(
			database.Cache().Stats,
			database.Cache().GetCacheSize,
		)
		collector.Start()
		defer collector.Stop()

		// Perform operations to generate metrics
		database.Put(ctx, "metrics/test", map[string]string{"foo": "bar"})
		var data map[string]string
		database.Get(ctx, "metrics/test", &data)

		// Update metrics
		collector.UpdateCacheMetrics()

		// Verify metrics can be exposed via HTTP handler
		handler := metrics.Handler()
		if handler == nil {
			t.Fatal("metrics.Handler() returned nil")
		}

		// Test that handler serves Prometheus format
		req := httptest.NewRequest("GET", "/metrics", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Handler returned %d, want %d", rec.Code, http.StatusOK)
		}

		body := rec.Body.String()
		if body == "" {
			t.Error("Metrics handler returned empty body")
		}

		// Verify Prometheus format (should contain metric names)
		expectedMetrics := []string{
			"jaydb_cache_hits_total",
			"jaydb_cache_misses_total",
			"jaydb_cache_size_bytes",
		}
		for _, metric := range expectedMetrics {
			if len(body) < len(metric) || !contains(body, metric) {
				t.Errorf("Metrics output missing expected metric: %s", metric)
			}
		}
	})

	t.Run("MultiNodeClustering", func(t *testing.T) {
		// Create partition ring (replicas=100, depth=2)
		ring := sharding.NewRing(100, 2)

		// Create first node's database
		db1, err := db.Open(db.Options{
			Storage:       memory.NewDriver(),
			ShardingDepth: 2,
		})
		if err != nil {
			t.Fatalf("Open db1 failed: %v", err)
		}
		defer db1.Close()

		// Port 0 lets the kernel assign free ports. Hardcoded ports collide with
		// the other packages' cluster tests, which `go test ./...` runs as
		// concurrent processes.
		node1, err := cluster.NewNode(cluster.NodeConfig{
			NodeName:  "test-node-1",
			BindAddr:  "127.0.0.1",
			BindPort:  0,
			QuicPort:  0,
			Ring:      ring,
			DBHandler: db1,
		})
		if err != nil {
			t.Fatalf("NewNode node1 failed: %v", err)
		}
		defer node1.Close()

		// The node must report the ports it actually bound, not the requested 0.
		if node1.QuicPort() == 0 {
			t.Error("QuicPort() = 0, want an OS-assigned port")
		}
		if node1.BindPort() == 0 {
			t.Error("BindPort() = 0, want an OS-assigned port")
		}

		wantSelf := fmt.Sprintf("127.0.0.1:%d", node1.QuicPort())
		if got := node1.SelfQuicAddr(); got != wantSelf {
			t.Errorf("SelfQuicAddr() = %q, want %q", got, wantSelf)
		}

		// Verify clustering components work
		if ring.PartitionDepth() != 2 {
			t.Errorf("Ring depth = %d, want 2", ring.PartitionDepth())
		}

		testKey := "users/123/profile"
		partition := sharding.PartitionKey(testKey, 2)
		if partition == "" {
			t.Error("PartitionKey returned empty string")
		}

		// NewNode registers itself in the ring, so the key must resolve to this
		// node's real QUIC address — which is what db routing compares against.
		targetNode := ring.GetNode(testKey)
		if targetNode != wantSelf {
			t.Errorf("Ring.GetNode(%q) = %q, want %q", testKey, targetNode, wantSelf)
		}
	})

	t.Run("StorageDrivers", func(t *testing.T) {
		drivers := []struct {
			name   string
			driver storage.Driver
		}{
			{"memory", memory.NewDriver()},
			// Note: S3 and FS drivers tested separately with real backends
		}

		for _, tc := range drivers {
			t.Run(tc.name, func(t *testing.T) {
				database, err := db.Open(db.Options{
					Storage: tc.driver,
				})
				if err != nil {
					t.Fatalf("Open with %s driver failed: %v", tc.name, err)
				}
				defer database.Close()

				// Basic smoke test
				data := map[string]string{"driver": tc.name}
				_, err = database.Put(ctx, "test/driver", data)
				if err != nil {
					t.Fatalf("Put with %s driver failed: %v", tc.name, err)
				}

				var readData map[string]string
				_, err = database.Get(ctx, "test/driver", &readData)
				if err != nil {
					t.Fatalf("Get with %s driver failed: %v", tc.name, err)
				}

				if readData["driver"] != tc.name {
					t.Errorf("Got %v, want %v", readData, data)
				}
			})
		}
	})

	t.Run("ShardingDepth", func(t *testing.T) {
		database, err := db.Open(db.Options{
			Storage:       memory.NewDriver(),
			ShardingDepth: 3,
		})
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		depth := database.ShardingDepth()
		if depth != 3 {
			t.Errorf("ShardingDepth = %d, want 3", depth)
		}

		// Verify partition key calculation works
		key := "users/alice/profile/main"
		partition := sharding.PartitionKey(key, 3)
		expected := "users/alice/profile"
		if partition != expected {
			t.Errorf("PartitionKey(%q, 3) = %q, want %q", key, partition, expected)
		}
	})
}

// contains checks if a string contains a substring (helper for older Go versions).
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Ensure cache.Manager implements expected interface for embedded usage.
var _ interface {
	Stats() (hits, misses, sfHits uint64)
	GetCacheSize() (items int, bytes int64)
	SetTTL(time.Duration)
	Invalidate(key string)
} = (*cache.Manager)(nil)
