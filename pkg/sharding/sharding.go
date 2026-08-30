package sharding

import (
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

// Ring manages deterministic, consensus-free sharding for cluster nodes.
// Active nodes are sorted deterministically so that every cluster member
// computes the identical shard distribution (N nodes = N shards) independently.
type Ring struct {
	mu             sync.RWMutex
	nodes          []string // Deterministically sorted slice of physical node addresses
	nodeSet        map[string]struct{}
	replicas       int // Retained for backwards compatibility
	partitionDepth int

	// generation increments on every real membership change so caches and DBs
	// can detect that ownership moved. It is read on every DB operation, so it
	// lives outside mu: taking the ring's RWMutex just to learn "nothing
	// changed" would put a lock acquisition on the hot path.
	generation atomic.Uint64
}

// NewRing initializes a deterministic sharding ring with configurable partition depth.
func NewRing(replicas int, partitionDepth int) *Ring {
	if replicas <= 0 {
		replicas = 100
	}
	return &Ring{
		nodes:          make([]string, 0),
		nodeSet:        make(map[string]struct{}),
		replicas:       replicas,
		partitionDepth: partitionDepth,
	}
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// AddNode registers a physical node address (e.g., "10.0.0.1:8080") into the ring.
// Nodes are maintained in deterministic sorted order.
func (r *Ring) AddNode(nodeAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodeSet[nodeAddr]; exists {
		return
	}
	r.nodeSet[nodeAddr] = struct{}{}
	r.nodes = append(r.nodes, nodeAddr)
	sort.Strings(r.nodes)

	// Bumped only past the early return above: a re-add of a node already in
	// the ring changes no ownership.
	r.generation.Add(1)
}

// RemoveNode unregisters a node from the ring.
func (r *Ring) RemoveNode(nodeAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodeSet[nodeAddr]; !exists {
		return
	}
	delete(r.nodeSet, nodeAddr)

	newNodes := make([]string, 0, len(r.nodes)-1)
	for _, n := range r.nodes {
		if n != nodeAddr {
			newNodes = append(newNodes, n)
		}
	}
	r.nodes = newNodes
	// Slice is already sorted since r.nodes was sorted and we only removed an item.

	r.generation.Add(1)
}

// SetNodes bulk-replaces the active nodes in the ring with the given set.
// It sorts and deduplicates the list deterministically. If the membership
// did not change, generation is not bumped.
func (r *Ring) SetNodes(nodeAddrs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	dedup := make(map[string]struct{}, len(nodeAddrs))
	for _, addr := range nodeAddrs {
		if addr != "" {
			dedup[addr] = struct{}{}
		}
	}

	newNodes := make([]string, 0, len(dedup))
	for addr := range dedup {
		newNodes = append(newNodes, addr)
	}
	sort.Strings(newNodes)

	// Check if unchanged
	if len(newNodes) == len(r.nodes) {
		identical := true
		for i := range newNodes {
			if newNodes[i] != r.nodes[i] {
				identical = false
				break
			}
		}
		if identical {
			return
		}
	}

	r.nodes = newNodes
	r.nodeSet = dedup
	r.generation.Add(1)
}

// Generation returns a counter that increments on every membership change.
// Callers cache ownership decisions (which node owns which key) and use this to
// notice when those decisions went stale, so it is deliberately cheap enough to
// read on every operation.
func (r *Ring) Generation() uint64 {
	return r.generation.Load()
}

// ShardFor returns the 0-based shard index for the given key based on lexical partition depth.
// Returns -1 if no nodes exist in the ring.
func (r *Ring) ShardFor(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return -1
	}

	pkey := PartitionKey(key, r.partitionDepth)
	h := hashString(pkey)
	return int(h % uint32(len(r.nodes)))
}

// GetNode returns the physical node address responsible for the given key based on lexical partition depth.
// Deterministic: N nodes = N shards; node = sortedNodes[hash(key) % N].
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return ""
	}

	pkey := PartitionKey(key, r.partitionDepth)
	h := hashString(pkey)
	idx := int(h % uint32(len(r.nodes)))
	return r.nodes[idx]
}

// NodeIndex returns the 0-based deterministic index of the given node in the sorted cluster,
// or -1 if the node is not registered.
func (r *Ring) NodeIndex(nodeAddr string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	idx := sort.SearchStrings(r.nodes, nodeAddr)
	if idx < len(r.nodes) && r.nodes[idx] == nodeAddr {
		return idx
	}
	return -1
}

// IsOwner reports whether the given node address is the designated shard owner for the key.
func (r *Ring) IsOwner(key string, selfAddr string) bool {
	return r.GetNode(key) == selfAddr
}

// NodeCount returns the number of active nodes (and thus the number of shards).
func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// Nodes returns a snapshot copy of the deterministically sorted node addresses.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make([]string, len(r.nodes))
	copy(cp, r.nodes)
	return cp
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
	_, ok := r.nodeSet[nodeAddr]
	return ok
}
