package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/metrics"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

func main() {
	ctx := context.Background()

	fmt.Println("=== JayDB Embedded Mode Example ===")
	fmt.Println("This example demonstrates using JayDB as a Go library")
	fmt.Println("without running the HTTP server.\n")

	// 1. Initialize in-memory database (pure embedded mode)
	database, err := db.Open(db.Options{
		Storage:       memory.NewDriver(),
		ShardingDepth: 2,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// 2. Set up metrics collector (works without server)
	collector := metrics.NewCollector(
		database.Cache().Stats,
		database.Cache().GetCacheSize,
	)
	collector.Start()
	defer collector.Stop()

	// 3. Optionally expose metrics on a standalone HTTP port
	//    (This is independent of the JayDB server - use your own server or skip it entirely)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		fmt.Println("Prometheus metrics available at: http://localhost:9090/metrics\n")
		if err := http.ListenAndServe(":9090", mux); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// 4. Use the database directly in your application code
	fmt.Println("Creating users...")
	for i := 1; i <= 5; i++ {
		user := User{
			Name:  fmt.Sprintf("User %d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
			Age:   20 + i,
		}
		meta, err := database.Put(ctx, fmt.Sprintf("users/%d", i), user)
		if err != nil {
			log.Printf("Put error: %v", err)
			continue
		}
		fmt.Printf("✓ Created user %d (ETag: %s)\n", i, meta.ETag)
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\nReading users (testing cache hits)...")
	for i := 1; i <= 10; i++ {
		var user User
		key := fmt.Sprintf("users/%d", (i%5)+1)
		_, err := database.Get(ctx, key, &user)
		if err != nil {
			log.Printf("Get error: %v", err)
			continue
		}
		fmt.Printf("✓ Read %s (age: %d, cached: %v)\n", user.Name, user.Age, i > 5)
		time.Sleep(50 * time.Millisecond)
	}

	// 5. Demonstrate CAS (Compare-And-Swap) optimistic locking
	fmt.Println("\nTesting CAS update...")
	var user User
	originalMeta, err := database.Get(ctx, "users/1", &user)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Original: %s (age: %d, ETag: %s)\n", user.Name, user.Age, originalMeta.ETag)

	user.Age = 99
	newMeta, err := database.Put(ctx, "users/1", user, db.WithExpectedETag(originalMeta.ETag))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Updated to age %d (new ETag: %s)\n", user.Age, newMeta.ETag)

	// Attempt conflicting update (should fail)
	user.Age = 100
	_, err = database.Put(ctx, "users/1", user, db.WithExpectedETag(originalMeta.ETag))
	if err != nil {
		fmt.Printf("✓ CAS conflict detected correctly: %v\n", err)
	}

	// 6. List keys by prefix
	fmt.Println("\nListing all users...")
	items, err := database.List(ctx, "users/", 100)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d users\n", len(items))

	// 7. Display cache statistics
	fmt.Println("\n=== Cache Statistics ===")
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for i := 0; i < 3; i++ {
		collector.UpdateCacheMetrics()

		hits, misses, sfHits := database.Cache().Stats()
		items, bytes := database.Cache().GetCacheSize()

		fmt.Printf("\nIteration %d:\n", i+1)
		fmt.Printf("  Cache Hits: %d\n", hits)
		fmt.Printf("  Cache Misses: %d\n", misses)
		fmt.Printf("  Singleflight Coalesced: %d\n", sfHits)
		fmt.Printf("  Items in Cache: %d\n", items)
		fmt.Printf("  Cache Size: %d bytes (~%.2f KB)\n", bytes, float64(bytes)/1024)

		if hits+misses > 0 {
			hitRate := float64(hits) / float64(hits+misses) * 100
			fmt.Printf("  Hit Rate: %.1f%%\n", hitRate)
		}

		if i < 2 {
			<-ticker.C
		}
	}

	fmt.Println("\n✓ Embedded mode demonstration complete!")
	fmt.Println("Check http://localhost:9090/metrics for Prometheus metrics")
	fmt.Println("\nPress Ctrl+C to exit")

	// Keep running to allow metrics inspection
	select {}
}
