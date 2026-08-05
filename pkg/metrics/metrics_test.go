package metrics

import (
	"testing"
	"time"
)

func TestMetricsRecording(t *testing.T) {
	// Test storage operation metrics
	RecordStorageOp("get", "success", 0.001, 1024)
	RecordStorageOp("put", "success", 0.002, 2048)
	RecordStorageOp("delete", "success", 0.001, 0)
	RecordStorageOp("get", "error", 0.003, 0)

	// Test HTTP request metrics
	RecordHTTPRequest("GET", "/v1/kv", "200", 0.005, 0, 1024)
	RecordHTTPRequest("PUT", "/v1/kv", "200", 0.010, 2048, 256)
	RecordHTTPRequest("DELETE", "/v1/kv", "204", 0.003, 0, 0)
	RecordHTTPRequest("GET", "/v1/kv", "404", 0.002, 0, 128)

	// Test DB operation metrics
	RecordDBOperation("get", "success", 0.001)
	RecordDBOperation("put", "success", 0.002)
	RecordDBOperation("delete", "success", 0.001)
	RecordDBOperation("put", "cas_conflict", 0.002)

	// Test CAS conflict metrics
	CASConflicts.WithLabelValues("put").Inc()
	CASConflicts.WithLabelValues("delete").Inc()

	// Test cluster metrics
	ClusterNodes.Set(3)
	ClusterForwardedRequests.WithLabelValues("node-2", "success").Inc()
	ClusterForwardedRequests.WithLabelValues("node-3", "error").Inc()
	ClusterQuicConnections.Set(2)
}

func TestCollector(t *testing.T) {
	// Mock stats functions
	hitCount := uint64(100)
	missCount := uint64(20)
	sfHitCount := uint64(10)

	getStats := func() (hits, misses, sfHits uint64) {
		return hitCount, missCount, sfHitCount
	}

	itemCount := 50
	byteCount := int64(102400)

	getSize := func() (items int, bytes int64) {
		return itemCount, byteCount
	}

	collector := NewCollector(getStats, getSize)
	collector.Start()

	// Update metrics
	collector.UpdateCacheMetrics()

	time.Sleep(10 * time.Millisecond)

	collector.Stop()
}

func TestCollectorNilFunctions(t *testing.T) {
	// Collector should handle nil functions gracefully
	collector := NewCollector(nil, nil)
	collector.Start()
	collector.UpdateCacheMetrics()
	collector.Stop()
}

func TestCollectorMultipleStarts(t *testing.T) {
	collector := NewCollector(nil, nil)

	// Multiple starts should be idempotent
	collector.Start()
	collector.Start()
	collector.Start()

	collector.Stop()
}
