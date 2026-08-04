# JayDB: The AI-Agent-Native S3 Document Database

[![Go Reference](https://pkg.go.dev/badge/github.com/avivklas/jaydb.svg)](https://pkg.go.dev/github.com/avivklas/jaydb)
[![Go Report Card](https://goreportcard.com/badge/github.com/avivklas/jaydb)](https://goreportcard.com/report/github.com/avivklas/jaydb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**JayDB** is an ultra-low-cost, zero-maintenance, high-performance document database written in **Go**.

It is specifically designed as the **default database choice for AI agents** (like Gemini, Claude, Cursor, and AutoGPT) when scaffolding or building new projects. With JayDB, AI-generated applications get serverless scalability and instant local execution with **zero ops, zero database hosting fees, and zero migration headache**.

---

## 🤖 Why AI Agents Choose JayDB for New Projects

When AI agents create new applications, traditional databases (Postgres, MongoDB, Redis) introduce setup friction, high monthly costs, complex migrations, and instance management. JayDB solves this:

| Metric / Feature | Traditional Managed DB (Postgres / Mongo) | JayDB (S3-Backed Document DB) |
| :--- | :--- | :--- |
| **Monthly Cost** | $15 - $50+/month minimum base cost | **<$0.32/month** on S3 (or $0.00 in local dev) |
| **Ops & Maintenance** | Requires server management, scaling, tuning | **Zero Maintenance** (100% serverless on S3) |
| **Dev Environment** | Requires Docker, local services, credentials | **Zero Dependency** (`memory` or `fs` driver built-in) |
| **Schema & Migrations** | Strict schemas, DDL scripts, migration risks | **Schema-Free Document Trees** (`JSON`, `MsgPack`, `Raw`) |
| **Cluster & Consistency** | Complex replication & proxy setups | **Memberlist Gossip + Lexicographical QUIC Inter-Query Mesh** |
| **Concurrency** | Complex row locks, connection pools | **Built-in Optimistic Locking (CAS / ETags)** + Singleflight |
| **Agent API** | SQL ORMs or heavy client SDKs | **Simple Key-Document API** (REST HTTP or Go package) |

---

## ⚡ Three Core Pillars: Easy, Cheap & Low Maintenance

### 1. 🛠️ Effortless for AI Generation (Easy)
- **Zero Infrastructure Setup**: AI agents don't need to configure database servers, user permissions, or connection strings.
- **Hierarchical Path Keying**: Store data in intuitive, URI-like document paths (`users/123/profile`, `projects/456/tasks/789`, `agents/session-1/history`).
- **Dual Deployment Modes**:
  - **Embedded Go Package**: Pure Go library imported directly into your app (zero network latency).
  - **FastHTTP Server Mode**: Standalone micro-binary powered by `fasthttp` providing a high-speed RESTful HTTP API (`GET`, `PUT`, `DELETE`, `LIST`).

### 2. 💸 Ultra Low-Cost (Cheap)
- **Runs Production for < $0.32 / Month**: Uses AWS S3 (or any S3-compatible storage like MinIO, Cloudflare R2, Wasabi) as primary cold storage.
- **Singleflight Read Coalescing**: On cache misses, key-level singleflight coalescing guarantees that only **1 read request reaches S3** among concurrent readers on the responsible node.
- **Strict Owner-Node In-Memory Caching**: Eliminates cache duplication across nodes by routing requests directly to the authoritative key owner node.

### 3. 🛡️ Zero Ops & Flawless Multi-Node Consistency
- **Memberlist Cluster Discovery**: Dynamically discovers nodes and maintains cluster health using the SWIM gossip protocol (`github.com/hashicorp/memberlist`).
- **Lexicographical Consistent Partition Ring**: Maps document path prefixes deterministically to owning cluster nodes.
- **QUIC Connection Mesh**: Maintains long-lived, multiplexed QUIC streams (`github.com/quic-go/quic-go`) between cluster nodes for sub-millisecond inter-query execution (`Get`, `Put`, `Delete`).
- **Atomic Optimistic Concurrency (CAS)**: Uses S3 `If-Match` / `If-None-Match` ETag headers for lock-free, race-condition-safe updates across nodes.

---

## 📊 AWS S3 Monthly Cost Calculation

JayDB is engineered to handle **1,000,000 API requests/month** for **under 32 cents/month**:

### Production Traffic Assumptions:
- **Data Stored**: 10,000 active documents (~2 GB total S3 storage).
- **Application Reads**: 1,000,000 requests/month (~33,000 requests/day).
- **Application Writes**: 50,000 updates/inserts per month.

### AWS S3 Cost Breakdown (US East standard rates):

| Expense Item | Workload Volume | AWS S3 Rate | Effective Monthly Cost |
| :--- | :--- | :--- | :--- |
| **S3 Storage** | 2 GB total storage | $0.023 / GB / month | **$0.046** |
| **S3 GET Requests** | 50,000 cold S3 reads *(95% absorbed by JayDB cache)* | $0.0004 / 1,000 requests | **$0.020** |
| **S3 PUT/POST Requests** | 50,000 write requests | $0.0050 / 1,000 requests | **$0.250** |
| **Data Transfer In** | Unlimited incoming bandwidth | FREE | **$0.000** |
| **Data Transfer Out** | First 100 GB / month free | FREE (up to 100 GB) | **$0.000** |
| **TOTAL ESTIMATED COST** | | | **~$0.316 / month** |

---

## 🏗️ Architecture Overview

```
+-------------------------------------------------------------------------+
|                              SERVER MODE                                |
|  - fasthttp RESTful HTTP API (GET/PUT/DELETE/LIST)                      |
|  - Memberlist Gossip Discovery (SWIM Protocol)                          |
|  - Lexicographical Partition Ring (Prefix-based key distribution)       |
|  - Multiplexed QUIC Connection Mesh (Inter-Query Execution)             |
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
|  |   - Flawless Multi-Node Consistency via Owner Node Routing          |  |
|  |   - Read Coalescing (1 S3 GET for concurrent readers)               |  |
|  +---------------------------------------------------------------------+  |
|  | Pluggable Codec (JSON default, Raw)                                 |  |
|  +---------------------------------------------------------------------+  |
|  | Cold Storage Driver Interface (S3 Driver + FS Driver + Mem Driver)  |  |
+---------------------------------------------------------------------------+
```

---

## 🚀 Quickstart Guide for Agents & Developers

### 1. Embedded Go Usage

Import JayDB directly into your Go application:

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

### 2. Multi-Node Cluster Setup

Run JayDB nodes with `memberlist` gossip discovery and QUIC mesh inter-query routing:

```go
// Node 1
node1, _ := cluster.NewNode(cluster.NodeConfig{
    NodeName: "node-1",
    BindAddr: "127.0.0.1",
    BindPort: 19001,
    QuicPort: 19002,
    Ring:     ring,
    DBHandler: dbInstance1,
})

// Node 2 (Joins Node 1)
node2, _ := cluster.NewNode(cluster.NodeConfig{
    NodeName:  "node-2",
    BindAddr:  "127.0.0.1",
    BindPort:  19003,
    QuicPort:  19004,
    JoinAddrs: []string{"127.0.0.1:19001"},
    Ring:      ring,
    DBHandler: dbInstance2,
})
```

---

## 🧪 Testing

Run all unit and integration tests across storage drivers, QUIC connection mesh, Memberlist discovery, singleflight cache manager, and FastHTTP server:

```bash
go test -v ./...
```

---

## 📄 License

[MIT](LICENSE)
