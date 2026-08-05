package metrics

import (
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Cache metrics
	CacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jaydb_cache_hits_total",
		Help: "Total number of cache hits",
	})

	CacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jaydb_cache_misses_total",
		Help: "Total number of cache misses",
	})

	CacheSingleflightHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jaydb_cache_singleflight_hits_total",
		Help: "Total number of coalesced requests (singleflight hits)",
	})

	CacheSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jaydb_cache_size_bytes",
		Help: "Current cache size in bytes",
	})

	CacheItems = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jaydb_cache_items",
		Help: "Current number of items in cache",
	})

	// Storage backend metrics
	StorageOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_storage_operations_total",
		Help: "Total number of storage operations by type and status",
	}, []string{"operation", "status"})

	StorageOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jaydb_storage_operation_duration_seconds",
		Help:    "Duration of storage operations in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	StorageBytesTransferred = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_storage_bytes_total",
		Help: "Total bytes transferred to/from storage backend",
	}, []string{"operation"})

	// HTTP server metrics
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_http_requests_total",
		Help: "Total number of HTTP requests by method, path prefix, and status",
	}, []string{"method", "path_prefix", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jaydb_http_request_duration_seconds",
		Help:    "Duration of HTTP requests in seconds",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"method", "path_prefix"})

	HTTPRequestSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "jaydb_http_request_size_bytes",
		Help:    "Size of HTTP request bodies in bytes",
		Buckets: prometheus.ExponentialBuckets(100, 10, 8),
	})

	HTTPResponseSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "jaydb_http_response_size_bytes",
		Help:    "Size of HTTP response bodies in bytes",
		Buckets: prometheus.ExponentialBuckets(100, 10, 8),
	})

	// Cluster metrics
	ClusterNodes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jaydb_cluster_nodes",
		Help: "Current number of nodes in the cluster",
	})

	ClusterForwardedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_cluster_forwarded_requests_total",
		Help: "Total number of forwarded requests by status",
	}, []string{"target_node", "status"})

	ClusterQuicConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jaydb_cluster_quic_connections",
		Help: "Current number of active QUIC connections",
	})

	// Database operation metrics
	DBOperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_db_operations_total",
		Help: "Total number of database operations by type and status",
	}, []string{"operation", "status"})

	DBOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jaydb_db_operation_duration_seconds",
		Help:    "Duration of database operations in seconds",
		Buckets: []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"operation"})

	// CAS conflict metrics
	CASConflicts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_cas_conflicts_total",
		Help: "Total number of CAS (compare-and-swap) conflicts by operation",
	}, []string{"operation"})

	// Key distribution metrics
	KeyDistribution = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jaydb_key_distribution",
		Help:    "Distribution of key path depths",
		Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}, []string{"shard"})
)

// Collector periodically updates gauge metrics from cache stats
type Collector struct {
	getStats func() (hits, misses, sfHits uint64)
	getSize  func() (items int, bytes int64)
	running  uint32
}

// NewCollector creates a metrics collector
func NewCollector(
	getStats func() (hits, misses, sfHits uint64),
	getSize func() (items int, bytes int64),
) *Collector {
	return &Collector{
		getStats: getStats,
		getSize:  getSize,
	}
}

// UpdateCacheMetrics syncs cache stats to Prometheus gauges/counters
func (c *Collector) UpdateCacheMetrics() {
	if c.getStats != nil {
		hits, misses, sfHits := c.getStats()
		CacheHits.Add(float64(hits))
		CacheMisses.Add(float64(misses))
		CacheSingleflightHits.Add(float64(sfHits))
	}

	if c.getSize != nil {
		items, bytes := c.getSize()
		CacheItems.Set(float64(items))
		CacheSize.Set(float64(bytes))
	}
}

// Start begins periodic metrics collection
func (c *Collector) Start() {
	if !atomic.CompareAndSwapUint32(&c.running, 0, 1) {
		return
	}
	// Metrics are exposed on-demand via /metrics endpoint
	// Real-time collection happens via cache.Stats() calls
}

// Stop halts metrics collection
func (c *Collector) Stop() {
	atomic.StoreUint32(&c.running, 0)
}

// RecordStorageOp records a storage operation metric
func RecordStorageOp(operation, status string, durationSecs float64, bytes int64) {
	StorageOpsTotal.WithLabelValues(operation, status).Inc()
	StorageOpDuration.WithLabelValues(operation).Observe(durationSecs)
	if bytes > 0 {
		StorageBytesTransferred.WithLabelValues(operation).Add(float64(bytes))
	}
}

// RecordHTTPRequest records an HTTP request metric
func RecordHTTPRequest(method, pathPrefix, status string, durationSecs float64, reqSize, respSize int) {
	HTTPRequestsTotal.WithLabelValues(method, pathPrefix, status).Inc()
	HTTPRequestDuration.WithLabelValues(method, pathPrefix).Observe(durationSecs)
	if reqSize > 0 {
		HTTPRequestSize.Observe(float64(reqSize))
	}
	if respSize > 0 {
		HTTPResponseSize.Observe(float64(respSize))
	}
}

// RecordDBOperation records a database operation metric
func RecordDBOperation(operation, status string, durationSecs float64) {
	DBOperationsTotal.WithLabelValues(operation, status).Inc()
	DBOperationDuration.WithLabelValues(operation).Observe(durationSecs)
}

// Handler returns a standard HTTP handler for exposing Prometheus metrics.
// Use this in embedded mode to serve metrics on your own HTTP server or port.
//
// Example (embedded mode with separate metrics port):
//
//	http.Handle("/metrics", metrics.Handler())
//	http.ListenAndServe(":9090", nil)
func Handler() http.Handler {
	return promhttp.Handler()
}
