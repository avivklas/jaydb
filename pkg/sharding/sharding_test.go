package sharding

import (
	"testing"
)

func TestPartitionKey(t *testing.T) {
	tests := []struct {
		key      string
		depth    int
		expected string
	}{
		{"users/123/posts/456", 1, "users"},
		{"users/123/posts/456", 2, "users/123"},
		{"users/123/posts/456", 3, "users/123/posts"},
		{"users/123/posts/456", 4, "users/123/posts/456"},
		{"users/123/posts/456", 0, "users/123/posts/456"},
	}

	for _, tt := range tests {
		got := PartitionKey(tt.key, tt.depth)
		if got != tt.expected {
			t.Errorf("PartitionKey(%s, %d) = '%s', expected '%s'", tt.key, tt.depth, got, tt.expected)
		}
	}
}

func TestRing_Consistency(t *testing.T) {
	ring := NewRing(50, 2)
	ring.AddNode("node1:8080")
	ring.AddNode("node2:8080")
	ring.AddNode("node3:8080")

	// Same prefix should ALWAYS map to exact same node
	key1 := "users/123/posts/1"
	key2 := "users/123/posts/2"
	key3 := "users/123/profile"

	node1 := ring.GetNode(key1)
	node2 := ring.GetNode(key2)
	node3 := ring.GetNode(key3)

	if node1 != node2 || node2 != node3 {
		t.Fatalf("expected all keys under 'users/123' to map to same node, got %s, %s, %s", node1, node2, node3)
	}
}

func TestDeterministicSharding(t *testing.T) {
	// Independent rings created in different orders must produce identical sorted node lists and shard mappings
	ringA := NewRing(1, 2)
	ringB := NewRing(1, 2)

	// Add in different orders
	ringA.AddNode("node-c:9000")
	ringA.AddNode("node-a:9000")
	ringA.AddNode("node-b:9000")

	ringB.AddNode("node-b:9000")
	ringB.AddNode("node-c:9000")
	ringB.AddNode("node-a:9000")

	if ringA.NodeCount() != 3 || ringB.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes, got A=%d, B=%d", ringA.NodeCount(), ringB.NodeCount())
	}

	nodesA := ringA.Nodes()
	nodesB := ringB.Nodes()
	expected := []string{"node-a:9000", "node-b:9000", "node-c:9000"}

	for i := range expected {
		if nodesA[i] != expected[i] || nodesB[i] != expected[i] {
			t.Fatalf("index %d: expected %s, got A=%s, B=%s", i, expected[i], nodesA[i], nodesB[i])
		}
	}

	// Verify NodeIndex
	for i, name := range expected {
		if idx := ringA.NodeIndex(name); idx != i {
			t.Fatalf("NodeIndex(%s) = %d, want %d", name, idx, i)
		}
	}
	if idx := ringA.NodeIndex("node-unknown"); idx != -1 {
		t.Fatalf("expected -1 for unknown node, got %d", idx)
	}

	// Test deterministic key mapping across both independent rings
	testKeys := []string{
		"users/1/data",
		"users/2/data",
		"orders/100/items",
		"products/999/info",
		"sessions/abc/token",
	}

	for _, k := range testKeys {
		nodeA := ringA.GetNode(k)
		nodeB := ringB.GetNode(k)
		if nodeA != nodeB {
			t.Fatalf("key %s mapped to different nodes: ringA=%s, ringB=%s", k, nodeA, nodeB)
		}
		shard := ringA.ShardFor(k)
		if shard < 0 || shard >= 3 {
			t.Fatalf("shard index out of bounds [0, 2]: %d", shard)
		}
		if ringA.Nodes()[shard] != nodeA {
			t.Fatalf("ShardFor (%d -> %s) does not match GetNode (%s)", shard, ringA.Nodes()[shard], nodeA)
		}
		if !ringA.IsOwner(k, nodeA) {
			t.Fatalf("IsOwner(%s, %s) should be true", k, nodeA)
		}
		if ringA.IsOwner(k, "wrong-node") {
			t.Fatalf("IsOwner should be false for wrong node")
		}
	}
}

func TestSetNodes(t *testing.T) {
	ring := NewRing(1, 2)
	ring.SetNodes([]string{"node-z:1", "node-a:1", "node-m:1"})

	if ring.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes, got %d", ring.NodeCount())
	}

	nodes := ring.Nodes()
	expected := []string{"node-a:1", "node-m:1", "node-z:1"}
	for i := range expected {
		if nodes[i] != expected[i] {
			t.Fatalf("at %d: expected %s, got %s", i, expected[i], nodes[i])
		}
	}

	genBefore := ring.Generation()
	// Calling SetNodes with same nodes in different order should not bump generation
	ring.SetNodes([]string{"node-m:1", "node-a:1", "node-z:1"})
	if ring.Generation() != genBefore {
		t.Fatalf("SetNodes with identical membership bumped generation")
	}

	// Calling SetNodes with a change should bump generation
	ring.SetNodes([]string{"node-a:1", "node-m:1"})
	if ring.Generation() != genBefore+1 {
		t.Fatalf("SetNodes with membership change did not bump generation")
	}
	if ring.NodeCount() != 2 {
		t.Fatalf("expected 2 nodes, got %d", ring.NodeCount())
	}
}
