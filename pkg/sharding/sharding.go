package sharding

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// PartitionKey extracts the partition prefix from a hierarchical key given a depth.
// For example, key="users/123/posts/456" with depth=2 returns "users/123".
func PartitionKey(key string, depth int) string {
	cleaned := strings.Trim(key, "/")
	if depth <= 0 {
		return cleaned
	}
	parts := strings.Split(cleaned, "/")
	if len(parts) <= depth {
		return cleaned
	}
	return strings.Join(parts[:depth], "/")
}

type virtualNode struct {
	hash     uint32
	nodeAddr string
}

// Ring manages consistent hashing for cluster nodes.
type Ring struct {
	mu             sync.RWMutex
	vnodes         []virtualNode
	replicas       int // Virtual nodes per physical node
	nodes          map[string]bool
	partitionDepth int

	// generation increments on every real membership change so caches can
	// detect that ownership moved. It is read on every DB operation, so it
	// lives outside mu: taking the ring's RWMutex just to learn "nothing
	// changed" would put a lock acquisition on the hot path.
	generation atomic.Uint64
}

// NewRing initializes a consistent hash ring with configurable virtual nodes and partition depth.
func NewRing(replicas int, partitionDepth int) *Ring {
	if replicas <= 0 {
		replicas = 100
	}
	return &Ring{
		replicas:       replicas,
		nodes:          make(map[string]bool),
		partitionDepth: partitionDepth,
	}
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// AddNode registers a physical node address (e.g., "10.0.0.1:8080") into the hash ring.
func (r *Ring) AddNode(nodeAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[nodeAddr] {
		return
	}
	r.nodes[nodeAddr] = true

	for i := 0; i < r.replicas; i++ {
		vKey := fmt.Sprintf("%s#%d", nodeAddr, i)
		h := hashString(vKey)
		r.vnodes = append(r.vnodes, virtualNode{hash: h, nodeAddr: nodeAddr})
	}

	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].hash < r.vnodes[j].hash
	})

	// Bumped only past the early return above: a re-add of a node already in
	// the ring changes no ownership, and signalling one would cost every DB in
	// the process a full cache purge for nothing.
	r.generation.Add(1)
}

// RemoveNode unregisters a node from the hash ring.
func (r *Ring) RemoveNode(nodeAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodes[nodeAddr] {
		return
	}
	delete(r.nodes, nodeAddr)

	var newVNodes []virtualNode
	for _, vn := range r.vnodes {
		if vn.nodeAddr != nodeAddr {
			newVNodes = append(newVNodes, vn)
		}
	}
	r.vnodes = newVNodes

	// Same reasoning as AddNode: only a real removal moves ownership.
	r.generation.Add(1)
}

// Generation returns a counter that increments on every membership change.
// Callers cache ownership decisions (which node owns which key) and use this to
// notice when those decisions went stale, so it is deliberately cheap enough to
// read on every operation.
func (r *Ring) Generation() uint64 {
	return r.generation.Load()
}

// GetNode returns the physical node address responsible for the given key based on lexical partition depth.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return ""
	}

	pkey := PartitionKey(key, r.partitionDepth)
	h := hashString(pkey)

	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})

	if idx == len(r.vnodes) {
		idx = 0
	}

	return r.vnodes[idx].nodeAddr
}

// PartitionDepth returns the ring's configured lexical partition depth.
func (r *Ring) PartitionDepth() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.partitionDepth
}

// HasNode checks if a node address is registered in the ring.
// Used for validating proxy forwarding targets.
func (r *Ring) HasNode(nodeAddr string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodes[nodeAddr]
}
