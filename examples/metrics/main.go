package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/metrics"
	"github.com/avivklas/jaydb/pkg/server"
	"github.com/avivklas/jaydb/pkg/storage/memory"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

func main() {
	ctx := context.Background()

	// 1. Initialize in-memory database
	database, err := db.Open(db.Options{
		Storage:       memory.NewDriver(),
		ShardingDepth: 2,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// 2. Set up metrics collector to sync cache stats to Prometheus
	collector := metrics.NewCollector(
		database.Cache().Stats,
		database.Cache().GetCacheSize,
	)
	collector.Start()
	defer collector.Stop()

	// 3. Create JayDB server (exposes /metrics endpoint automatically)
	srv, err := server.NewServer(server.Options{
		DB: database,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	// 4. Start JayDB HTTP server in background (includes /metrics endpoint)
	go func() {
		fmt.Println("JayDB server listening on :8080")
		fmt.Println("  - API: http://localhost:8080/v1/kv/<key>")
		fmt.Println("  - Health: http://localhost:8080/v1/health")
		fmt.Println("  - Metrics: http://localhost:8080/metrics")
		if err := srv.ListenAndServe(":8080"); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// 5. Optionally, expose metrics on a separate port (production pattern)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		fmt.Println("Standalone Prometheus metrics server listening on :9090")
		fmt.Println("  - Metrics: http://localhost:9090/metrics")
		if err := http.ListenAndServe(":9090", mux); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// 6. Simulate some database operations to generate metrics
	fmt.Println("\nGenerating sample metrics...")

	// Create some users
	for i := 1; i <= 10; i++ {
		user := User{
			Name:  fmt.Sprintf("User %d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
			Age:   20 + i,
		}
		meta, err := database.Put(ctx, fmt.Sprintf("users/%d", i), user)
		if err != nil {
			log.Printf("Put error: %v", err)
		} else {
			fmt.Printf("Created user %d (ETag: %s)\n", i, meta.ETag)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Read users (cache hits)
	for i := 1; i <= 20; i++ {
		var user User
		key := fmt.Sprintf("users/%d", (i%10)+1)
		_, err := database.Get(ctx, key, &user)
		if err != nil {
			log.Printf("Get error: %v", err)
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Update cache metrics periodically
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	fmt.Println("\nMetrics are being collected. Check:")
	fmt.Println("  - http://localhost:8080/metrics (JayDB server)")
	fmt.Println("  - http://localhost:9090/metrics (Standalone)")
	fmt.Println("\nPress Ctrl+C to exit")

	// Update and display metrics every 5 seconds
	for range ticker.C {
		collector.UpdateCacheMetrics()

		hits, misses, sfHits := database.Cache().Stats()
		items, bytes := database.Cache().GetCacheSize()

		fmt.Printf("\n=== Cache Stats ===\n")
		fmt.Printf("  Hits: %d\n", hits)
		fmt.Printf("  Misses: %d\n", misses)
		fmt.Printf("  Singleflight Hits: %d\n", sfHits)
		fmt.Printf("  Items: %d\n", items)
		fmt.Printf("  Size: %d bytes (~%.2f KB)\n", bytes, float64(bytes)/1024)

		if hits+misses > 0 {
			hitRate := float64(hits) / float64(hits+misses) * 100
			fmt.Printf("  Hit Rate: %.1f%%\n", hitRate)
		}
	}
}
