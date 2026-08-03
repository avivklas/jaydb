# JayDB (S3-Backed Sharded Cache Document Database)

[![Go Reference](https://pkg.go.dev/badge/github.com/avivklas/jaydb.svg)](https://pkg.go.dev/github.com/avivklas/jaydb)
[![Go Report Card](https://goreportcard.com/badge/github.com/avivklas/jaydb)](https://goreportcard.com/report/github.com/avivklas/jaydb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**JayDB** is a ultra low-cost (<$1/month on AWS), high-performance document database written in **Go**.

It is designed for simple, modern applications requiring hierarchical tree document storage, strong optimistic concurrency control (CAS), high memory cache performance via singleflight request coalescing and lexical index key sharding.

JayDB is built with an **embedded-first architecture**:
1. **Embedded Mode**: Clean, high-level Go package imported directly into your application (zero network overhead).
2. **Server Mode**: Lightweight standalone binary powered by `fasthttp` wrapping the embedded engine and handling multi-node cluster shard routing.

---

## Key Features

- **S3 Cold Storage with CAS**: Primary storage in AWS S3 (or S3-compatible backends like MinIO), leveraging native S3 `If-Match` / `If-None-Match` conditional headers for strong optimistic locking and near-zero base cost.
- **Singleflight Read Coalescing**: On cache miss, key-level singleflight guarantees that only **1 request** reaches cold storage among multiple concurrent read requests; remaining callers wait and receive the result of the first request.
- **Write-Through Cache & Key Mutex**: Successful writes update the in-memory cache immediately. Key-level locks serialize writes per key, invalidating cache entries on version conflicts.
- **Lexical Index Key Sharding**: User-configured path prefix partitioning (`ShardingDepth`) co-locates memory cache objects for related sub-keys (e.g. `users/123/posts/456` under `users/123`).
- **Pluggable Document Encoding**: Default `JSON` serializer, pluggable `Codec` interface (`MsgPack`, `Raw` byte slice option).
- **FastHTTP Server Layer**: Sub-millisecond RESTful API endpoints (`GET`, `PUT`, `DELETE`, `LIST`) powered by `valyala/fasthttp`.

---

## Cost Calculation (AWS S3 Monthly Costs)

JayDB is specifically engineered to run production workloads on AWS S3 for **less than $1/month**.

Below is a typical monthly cost breakdown for a normal production web application handling **1,000,000 API requests/month** and **50,000 document writes/month**.

### Production Traffic Assumptions:
- **Data Stored**: 10,000 active documents (~2 GB total S3 storage).
- **Application Reads**: 1,000,000 requests/month (approx 33,000 requests/day).
- **Application Writes**: 50,000 updates/inserts per month.

### AWS S3 Pricing Calculation (US Standard Rates):

| Expense Item | Workload Volume | AWS S3 Rate | Effective Monthly Cost |
| :--- | :--- | :--- | :--- |
| **S3 Storage** | 2 GB total storage | $0.023 / GB / month | **$0.046** |
| **S3 GET Requests** | 50,000 cold S3 reads *(95% cache hit rate via JayDB singleflight & sharding)* | $0.0004 / 1,000 requests | **$0.020** |
| **S3 PUT/POST Requests** | 50,000 write requests | $0.0050 / 1,000 requests | **$0.250** |
| **Data Transfer In** | Unlimited incoming bandwidth | FREE | **$0.000** |
| **Data Transfer Out** | First 100 GB / month free | FREE (up to 100 GB) | **$0.000** |
| **TOTAL ESTIMATED COST** | | | **~$0.316 / month** |

> [!NOTE]
> **Why is it so cheap?**
> Without JayDB's **in-memory sharded cache** and **singleflight request coalescing**, 1,000,000 S3 GET requests would cost **$0.40/mo**, and un-coalesced concurrent spikes could multiply S3 API charges.
> JayDB's cache absorbs 95%+ of read traffic in memory and coalesces spikes so **only 1 S3 request goes out**, keeping your AWS bill under **$0.32/month**.

---

## Architecture Overview

```
+-------------------------------------------------------------------------+
|                              SERVER MODE                                |
|  - fasthttp RESTful HTTP API (GET/PUT/DELETE/LIST)                      |
|  - Inter-Node Router (Proxying non-local shard keys over fasthttp)      |
+-------------------------------------------------------------------------+
                                    |
                                    v (Wraps internally)
+---------------------------------------------------------------------------+
|                             EMBEDDED MODE                                 |
|                         (Core Engine Library)                             |
|                                                                           |
|  +---------------------------------------------------------------------+  |
|  | High-Level Go API (Get, Put, Delete, List)                          |  |
|  +---------------------------------------------------------------------+  |
|  | Key-Level Mutex & Singleflight Cache Manager                        |  |
|  |   - Read Coalescing (1 S3 GET for concurrent readers)               |  |
|  |   - Write Population & CAS Invalidation                             |  |
|  +---------------------------------------------------------------------+  |
|  | Pluggable Codec (JSON default, Raw)                                 |  |
|  +---------------------------------------------------------------------+  |
|  | Cold Storage Driver Interface (S3 Driver + FS Driver + Mem Driver)  |  |
+---------------------------------------------------------------------------+
```

---

## Quickstart

### 1. Embedded Go Usage

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/storage/s3"
)

type UserProfile struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func main() {
	ctx := context.Background()

	// Initialize S3 storage driver
	store, err := s3.NewDriver(s3.Config{
		Bucket: "my-app-bucket",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Open JayDB embedded instance
	database, err := db.Open(db.Options{
		Storage:       store,
		ShardingDepth: 2, // Partition key prefix depth (e.g. "users/123")
	})
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// 1. Create document (If-None-Match: *)
	u := UserProfile{Name: "Alice", Email: "alice@example.com"}
	meta, err := database.Put(ctx, "users/123/profile", u, db.CreateOnly())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Created User ETag: %s\n", meta.ETag)

	// 2. Read document (Cached + Singleflight)
	var readUser UserProfile
	readMeta, err := database.Get(ctx, "users/123/profile", &readUser)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Read User: %+v (ETag: %s)\n", readUser, readMeta.ETag)

	// 3. Update document with CAS (If-Match: etag)
	u.Email = "alice-new@example.com"
	newMeta, err := database.Put(ctx, "users/123/profile", u, db.WithExpectedETag(readMeta.ETag))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Updated User ETag: %s\n", newMeta.ETag)
}
```

### 2. Standalone FastHTTP Server Mode

```bash
# Build CLI binary
go build -o jaydb ./cmd/jaydb

# Run memory-backed server
./jaydb -addr :8080 -storage memory -sharding-depth 2

# Run filesystem-backed server
./jaydb -addr :8080 -storage fs -fs-dir ./data

# Run S3-backed server
./jaydb -addr :8080 -storage s3 -s3-bucket my-bucket
```

#### HTTP API Usage:

- **Create Document**:
  ```bash
  curl -i -X PUT http://localhost:8080/v1/kv/users/123/profile \
       -H "If-None-Match: *" \
       -d '{"name":"Alice","email":"alice@example.com"}'
  ```

- **Get Document**:
  ```bash
  curl -i http://localhost:8080/v1/kv/users/123/profile
  ```

- **Update Document (CAS)**:
  ```bash
  curl -i -X PUT http://localhost:8080/v1/kv/users/123/profile \
       -H 'If-Match: "etag-value"' \
       -d '{"name":"Alice","email":"new@example.com"}'
  ```

- **List Subtree**:
  ```bash
  curl -i http://localhost:8080/v1/list/users/123
  ```

---

## Testing

Run all unit and integration tests across storage drivers, singleflight cache manager, sharding ring, and FastHTTP server:

```bash
go test -v ./...
```

---

## License

[MIT](LICENSE)
