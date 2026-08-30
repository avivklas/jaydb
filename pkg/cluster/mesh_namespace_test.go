package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
)

type nsHandler struct {
	name string
	objs map[string][]byte
}

func (h *nsHandler) GetRaw(ctx context.Context, key string) (*storage.Object, error) {
	v, ok := h.objs[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return &storage.Object{Key: key, Value: v, ETag: h.name}, nil
}

func (h *nsHandler) PutRaw(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	h.objs[key] = value
	return &storage.Object{Key: key, Value: value, ETag: h.name}, nil
}

func (h *nsHandler) DeleteRaw(ctx context.Context, key string, expectedETag string) error {
	if _, ok := h.objs[key]; !ok {
		return storage.ErrNotFound
	}
	delete(h.objs, key)
	return nil
}

// nilHandler answers successfully but with no object — the shape that used to
// serialize to an empty response and be read as "document not found".
type nilHandler struct{}

func (nilHandler) GetRaw(context.Context, string) (*storage.Object, error) { return nil, nil }
func (nilHandler) PutRaw(context.Context, string, []byte, string) (*storage.Object, error) {
	return nil, nil
}
func (nilHandler) DeleteRaw(context.Context, string, string) error { return nil }

func newServeNode(t *testing.T, secret string) *Node {
	t.Helper()
	return &Node{
		cfg: NodeConfig{ClusterSecret: secret},
		ctx: context.Background(),
	}
}

// The regression that caused the incident: an unserviceable request must NOT
// look like a missing document.
func TestServeUnknownNamespaceFailsLoud(t *testing.T) {
	n := newServeNode(t, "")
	n.RegisterHandler("main", &nsHandler{name: "main", objs: map[string][]byte{"users/1": []byte(`{}`)}})

	resp := n.serve(InterQueryReq{Namespace: "does-not-exist", Op: OpGet, Key: "users/1"})

	if resp.Err == "" {
		t.Fatal("expected an explicit error for an unregistered namespace, got an empty response")
	}
	if resp.Err == storage.ErrNotFound.Error() {
		t.Fatal("unregistered namespace must NOT be reported as ErrNotFound")
	}
	if !strings.Contains(resp.Err, "no DB handler registered") {
		t.Errorf("error should name the real cause, got %q", resp.Err)
	}
	if resp.Object != nil {
		t.Error("expected no object")
	}
}

// Requests are routed to the namespace that owns them, not to whichever DB
// happened to register last.
func TestServeRoutesByNamespace(t *testing.T) {
	n := newServeNode(t, "")
	n.RegisterHandler("main", &nsHandler{name: "main", objs: map[string][]byte{"k": []byte(`"from-main"`)}})
	n.RegisterHandler("other", &nsHandler{name: "other", objs: map[string][]byte{"k": []byte(`"from-other"`)}})

	for _, tc := range []struct{ ns, want string }{
		{"main", "from-main"},
		{"other", "from-other"},
	} {
		resp := n.serve(InterQueryReq{Namespace: tc.ns, Op: OpGet, Key: "k"})
		if resp.Err != "" {
			t.Fatalf("ns %s: unexpected error %q", tc.ns, resp.Err)
		}
		if got := string(resp.Object.Value); got != `"`+tc.want+`"` {
			t.Errorf("ns %s: served %s, want %s", tc.ns, got, tc.want)
		}
		if resp.Object.ETag != tc.ns {
			t.Errorf("ns %s: served by the wrong namespace handler (%s)", tc.ns, resp.Object.ETag)
		}
	}
}

// A real miss still maps to ErrNotFound so callers keep working.
func TestServeGenuineMissIsNotFound(t *testing.T) {
	n := newServeNode(t, "")
	n.RegisterHandler("main", &nsHandler{name: "main", objs: map[string][]byte{}})

	resp := n.serve(InterQueryReq{Namespace: "main", Op: OpGet, Key: "absent"})
	if resp.Err != storage.ErrNotFound.Error() {
		t.Fatalf("expected ErrNotFound, got %q", resp.Err)
	}
}

// A handler returning (nil, nil) must not produce an empty response.
func TestServeNeverReturnsEmptyResponse(t *testing.T) {
	n := newServeNode(t, "")
	n.RegisterHandler("main", nilHandler{})

	for _, op := range []OpType{OpGet, OpPut} {
		resp := n.serve(InterQueryReq{Namespace: "main", Op: op, Key: "k"})
		if resp.Err == "" && resp.Object == nil {
			t.Errorf("op %d produced an empty response (neither object nor error)", op)
		}
	}
}

// Legacy single-DB callers that send no namespace keep working.
func TestServeEmptyNamespaceUsesLegacyHandler(t *testing.T) {
	n := &Node{
		cfg: NodeConfig{DBHandler: &nsHandler{name: "legacy", objs: map[string][]byte{"k": []byte(`1`)}}},
		ctx: context.Background(),
	}
	resp := n.serve(InterQueryReq{Op: OpGet, Key: "k"})
	if resp.Err != "" {
		t.Fatalf("legacy handler path failed: %q", resp.Err)
	}
	if resp.Object.ETag != "legacy" {
		t.Errorf("served by %q, want legacy", resp.Object.ETag)
	}
}

func TestUnregisterHandlerStopsServing(t *testing.T) {
	n := newServeNode(t, "")
	h := &nsHandler{name: "main", objs: map[string][]byte{"k": []byte(`1`)}}
	n.RegisterHandler("main", h)
	if resp := n.serve(InterQueryReq{Namespace: "main", Op: OpGet, Key: "k"}); resp.Err != "" {
		t.Fatalf("precondition failed: %q", resp.Err)
	}

	n.UnregisterHandler("main")
	resp := n.serve(InterQueryReq{Namespace: "main", Op: OpGet, Key: "k"})
	if !strings.Contains(resp.Err, "no DB handler registered") {
		t.Errorf("after unregister expected a loud error, got %q", resp.Err)
	}
}

func TestAuthentication(t *testing.T) {
	const secret = "s3cret"
	n := newServeNode(t, secret)
	n.RegisterHandler("main", &nsHandler{name: "main", objs: map[string][]byte{"k": []byte(`1`)}})

	signed := func(r InterQueryReq) InterQueryReq {
		r.TS = time.Now().Unix()
		r.Auth = computeAuth(secret, r)
		return r
	}

	t.Run("correctly signed request is served", func(t *testing.T) {
		if resp := n.serve(signed(InterQueryReq{Namespace: "main", Op: OpGet, Key: "k"})); resp.Err != "" {
			t.Fatalf("signed request rejected: %q", resp.Err)
		}
	})

	t.Run("unsigned request is rejected", func(t *testing.T) {
		resp := n.serve(InterQueryReq{Namespace: "main", Op: OpGet, Key: "k"})
		if resp.Err != ErrUnauthenticated.Error() {
			t.Fatalf("expected ErrUnauthenticated, got %q", resp.Err)
		}
	})

	t.Run("wrong secret is rejected", func(t *testing.T) {
		r := InterQueryReq{Namespace: "main", Op: OpGet, Key: "k"}
		r.TS = time.Now().Unix()
		r.Auth = computeAuth("wrong-secret", r)
		if resp := n.serve(r); resp.Err != ErrUnauthenticated.Error() {
			t.Fatalf("expected ErrUnauthenticated, got %q", resp.Err)
		}
	})

	t.Run("tampering with the key invalidates the signature", func(t *testing.T) {
		r := signed(InterQueryReq{Namespace: "main", Op: OpGet, Key: "k"})
		r.Key = "someone-elses-key" // attacker swaps the target
		if resp := n.serve(r); resp.Err != ErrUnauthenticated.Error() {
			t.Fatalf("tampered key accepted (got %q)", resp.Err)
		}
	})

	t.Run("tampering with the op invalidates the signature", func(t *testing.T) {
		r := signed(InterQueryReq{Namespace: "main", Op: OpGet, Key: "k"})
		r.Op = OpDelete // attacker upgrades a read into a delete
		if resp := n.serve(r); resp.Err != ErrUnauthenticated.Error() {
			t.Fatalf("tampered op accepted (got %q)", resp.Err)
		}
	})

	t.Run("stale request is rejected", func(t *testing.T) {
		r := InterQueryReq{Namespace: "main", Op: OpGet, Key: "k"}
		r.TS = time.Now().Add(-2 * authSkew).Unix()
		r.Auth = computeAuth(secret, r)
		if resp := n.serve(r); resp.Err != ErrUnauthenticated.Error() {
			t.Fatalf("replayed stale request accepted (got %q)", resp.Err)
		}
	})
}

// With no secret configured the mesh stays open, for local development.
func TestNoSecretSkipsAuth(t *testing.T) {
	n := newServeNode(t, "")
	n.RegisterHandler("main", &nsHandler{name: "main", objs: map[string][]byte{"k": []byte(`1`)}})
	if resp := n.serve(InterQueryReq{Namespace: "main", Op: OpGet, Key: "k"}); resp.Err != "" {
		t.Fatalf("unexpected error with no secret: %q", resp.Err)
	}
}

// A dial to an unreachable peer must be bounded by DialTimeout rather than by
// the 30s QUIC idle timeout, and it must not be serialized behind a node-wide
// lock. 203.0.113.0/24 is TEST-NET-3: reserved and black-holed.
func TestDialIsBoundedAndConcurrent(t *testing.T) {
	node, err := NewNode(NodeConfig{
		NodeName:    "dial-timeout-test",
		BindAddr:    "127.0.0.1",
		BindPort:    0,
		QuicPort:    0,
		Ring:        sharding.NewRing(16, 2),
		DialTimeout: 400 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	defer node.Close()

	// Concurrent dials to DIFFERENT dead peers must overlap, not queue. Serialized
	// dials would take ~N*DialTimeout; the old code held the node mutex while
	// dialing, so one dead peer stalled everything.
	const peers = 4
	start := time.Now()
	done := make(chan error, peers)
	for i := 0; i < peers; i++ {
		go func(i int) {
			_, _, err := node.meshPool.GetStream(context.Background(), fmt.Sprintf("203.0.113.%d:9999", i+1))
			done <- err
		}(i)
	}
	for i := 0; i < peers; i++ {
		if err := <-done; err == nil {
			t.Error("expected a dial error to a black-holed address")
		}
	}
	elapsed := time.Since(start)

	if elapsed > 3*time.Second {
		t.Errorf("dials took %v; expected them to be bounded (~400ms) and concurrent", elapsed)
	}
	t.Logf("%d concurrent dials to black-holed peers completed in %v", peers, elapsed)
}

// The caller's context must cancel a dial in flight.
func TestDialHonoursCallerContext(t *testing.T) {
	node, err := NewNode(NodeConfig{
		NodeName: "dial-ctx-test",
		BindAddr: "127.0.0.1",
		BindPort: 0,
		QuicPort: 0,
		Ring:     sharding.NewRing(16, 2),
		// Long dial timeout so the caller's context is what ends it.
		DialTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	defer node.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, _, err := node.meshPool.GetStream(ctx, "203.0.113.9:9999"); err == nil {
		t.Fatal("expected an error")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("dial ignored the caller's context: took %v", elapsed)
	}
}
