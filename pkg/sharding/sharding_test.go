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
