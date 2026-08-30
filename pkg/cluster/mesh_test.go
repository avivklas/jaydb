package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

type mockHandler struct {
	store storage.Driver
}

func (m *mockHandler) GetRaw(ctx context.Context, key string) (*storage.Object, error) {
	return m.store.Get(ctx, key)
}

func (m *mockHandler) PutRaw(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	return m.store.Put(ctx, key, value, expectedETag)
}

func (m *mockHandler) DeleteRaw(ctx context.Context, key string, expectedETag string) error {
	return m.store.Delete(ctx, key, expectedETag)
}

func TestMemberlistAndQuicMesh(t *testing.T) {
	ring := sharding.NewRing(10, 2)
	memStore := memory.NewDriver()

	// Seed object in store
	_, err := memStore.Put(context.Background(), "users/123/profile", []byte(`{"name":"Alice"}`), "")
	if err != nil {
		t.Fatalf("failed to seed store: %v", err)
	}

	h := &mockHandler{store: memStore}

	// Node 1. Port 0 = OS-assigned, so parallel test packages cannot collide.
	cfg1 := NodeConfig{
		NodeName:  "node-1",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		QuicPort:  0,
		Ring:      ring,
		DBHandler: h,
		PoolSize:  2,
	}
	node1, err := NewNode(cfg1)
	if err != nil {
		t.Fatalf("failed to create node1: %v", err)
	}
	defer node1.Close()

	// Node 2 (Joins Node 1). node1's gossip port is only known once it is bound,
	// so the join address has to be read back from the node itself.
	cfg2 := NodeConfig{
		NodeName:  "node-2",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		QuicPort:  0,
		JoinAddrs: []string{node1.GossipAddr()},
		Ring:      ring,
		DBHandler: h,
		PoolSize:  2,
	}
	node2, err := NewNode(cfg2)
	if err != nil {
		t.Fatalf("failed to create node2: %v", err)
	}
	defer node2.Close()

	// Wait brief moment for memberlist join sync and proactive pool warmup
	time.Sleep(500 * time.Millisecond)

	// Verify memberlist understanding
	if count := node1.MemberCount(); count != 2 {
		t.Errorf("node1 member count = %d, want 2", count)
	}
	if count := node2.MemberCount(); count != 2 {
		t.Errorf("node2 member count = %d, want 2", count)
	}

	members := node1.Members()
	if len(members) != 2 {
		t.Fatalf("node1 members len = %d, want 2", len(members))
	}

	// Verify sharding ring derived from memberlist
	if ring.NodeCount() != 2 {
		t.Errorf("ring node count = %d, want 2", ring.NodeCount())
	}

	// Execute QUIC Inter-Query from Node 2 to Node 1
	target := node1.SelfQuicAddr()
	req := InterQueryReq{
		Op:  OpGet,
		Key: "users/123/profile",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := node2.ExecuteInterQuery(ctx, target, req)
	if err != nil {
		t.Fatalf("inter-query failed: %v", err)
	}

	if resp.Err != "" {
		t.Fatalf("unexpected error response: %s", resp.Err)
	}

	if resp.Object == nil || string(resp.Object.Value) != `{"name":"Alice"}` {
		t.Fatalf("unexpected object value: %+v", resp.Object)
	}

	// Verify mesh pool has pre-opened connections
	if node2.MeshPool().PeerCount() != 1 {
		t.Errorf("node2 peer count = %d, want 1", node2.MeshPool().PeerCount())
	}
}

func TestMeshPoolLifecycleOnNodeDeparture(t *testing.T) {
	ring := sharding.NewRing(1, 2)
	node1, err := NewNode(NodeConfig{
		NodeName: "leave-node-1",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		QuicPort: 0,
		Ring:     ring,
		PoolSize: 2,
	})
	if err != nil {
		t.Fatalf("create node1: %v", err)
	}
	defer node1.Close()

	node2, err := NewNode(NodeConfig{
		NodeName:  "leave-node-2",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		QuicPort:  0,
		JoinAddrs: []string{node1.GossipAddr()},
		Ring:      ring,
		PoolSize:  2,
	})
	if err != nil {
		t.Fatalf("create node2: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if node1.MemberCount() != 2 {
		t.Fatalf("expected 2 members, got %d", node1.MemberCount())
	}

	// Close node2
	_ = node2.Close()

	time.Sleep(500 * time.Millisecond)

	// After node2 leaves, node1 should update sharding and clean up the peer pool
	targetQuic := node2.SelfQuicAddr()
	if ring.HasNode(targetQuic) {
		t.Errorf("ring still has departed node %s", targetQuic)
	}
}

