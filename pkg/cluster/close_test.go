package cluster

import (
	"fmt"
	"sync"
	"testing"

	"github.com/avivklas/jaydb/pkg/sharding"
)

// newTestNode starts a node on OS-assigned ports so concurrent test packages
// cannot collide on a fixed port.
func newTestNode(t *testing.T, name string, ring *sharding.Ring) *Node {
	t.Helper()

	node, err := NewNode(NodeConfig{
		NodeName:  name,
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		QuicPort:  0,
		Ring:      ring,
		DBHandler: &mockHandler{store: nil},
	})
	if err != nil {
		t.Fatalf("NewNode(%s): %v", name, err)
	}
	return node
}

// TestNodeClose_Idempotent guards against a regression that panicked the whole
// process: memberlist panics with "leave after shutdown" if Leave runs after
// Shutdown, and a Node is closed twice in ordinary use because db.Close also
// closes the ClusterNode it was configured with. A caller holding both a Node
// and a DB therefore has no shutdown order that closes the node exactly once.
func TestNodeClose_Idempotent(t *testing.T) {
	node := newTestNode(t, "close-idempotent", sharding.NewRing(100, 2))

	if err := node.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// Before the fix this panicked rather than returning.
	if err := node.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("third Close: %v", err)
	}
}

// TestNodeClose_ConcurrentIdempotent covers two goroutines racing to close the
// same node, which is what a deferred db.Close and node.Close can amount to.
func TestNodeClose_ConcurrentIdempotent(t *testing.T) {
	node := newTestNode(t, "close-concurrent", sharding.NewRing(100, 2))

	const closers = 8
	var wg sync.WaitGroup
	wg.Add(closers)

	for i := 0; i < closers; i++ {
		go func() {
			defer wg.Done()
			if err := node.Close(); err != nil {
				t.Errorf("concurrent Close: %v", err)
			}
		}()
	}

	wg.Wait()
}

// TestNodeEphemeralPorts verifies that requesting port 0 yields real bound ports
// and that every address the node advertises reflects them. If the node kept
// advertising the configured 0, ring registration and db routing would compare
// against "127.0.0.1:0" and never match.
func TestNodeEphemeralPorts(t *testing.T) {
	ring := sharding.NewRing(100, 2)
	node := newTestNode(t, "ephemeral", ring)
	defer node.Close()

	if node.QuicPort() == 0 {
		t.Error("QuicPort() = 0, want an OS-assigned port")
	}
	if node.BindPort() == 0 {
		t.Error("BindPort() = 0, want an OS-assigned port")
	}
	if node.QuicPort() == node.BindPort() {
		t.Errorf("QUIC and gossip ports must differ, both = %d", node.QuicPort())
	}

	wantQuic := fmt.Sprintf("127.0.0.1:%d", node.QuicPort())
	if got := node.SelfQuicAddr(); got != wantQuic {
		t.Errorf("SelfQuicAddr() = %q, want %q", got, wantQuic)
	}

	wantGossip := fmt.Sprintf("127.0.0.1:%d", node.BindPort())
	if got := node.GossipAddr(); got != wantGossip {
		t.Errorf("GossipAddr() = %q, want %q", got, wantGossip)
	}

	// NewNode must register the resolved address, not the requested port 0.
	if got := ring.GetNode("users/1/profile"); got != wantQuic {
		t.Errorf("ring resolved %q, want %q", got, wantQuic)
	}
}

// TestNodeEphemeralPorts_NoCollision starts several nodes at once; with OS
// assignment each must get a distinct port instead of failing to bind.
func TestNodeEphemeralPorts_NoCollision(t *testing.T) {
	const nodes = 4

	seen := make(map[int]string, nodes*2)
	for i := 0; i < nodes; i++ {
		name := fmt.Sprintf("no-collision-%d", i)
		node := newTestNode(t, name, sharding.NewRing(100, 2))
		defer node.Close()

		for label, port := range map[string]int{"quic": node.QuicPort(), "gossip": node.BindPort()} {
			if owner, dup := seen[port]; dup {
				t.Errorf("port %d assigned to both %s and %s/%s", port, owner, name, label)
			}
			seen[port] = name + "/" + label
		}
	}
}
