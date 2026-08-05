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
	}
	node2, err := NewNode(cfg2)
	if err != nil {
		t.Fatalf("failed to create node2: %v", err)
	}
	defer node2.Close()

	// Wait brief moment for memberlist join sync
	time.Sleep(500 * time.Millisecond)

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
}
