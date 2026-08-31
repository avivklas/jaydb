package cluster

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/hashicorp/memberlist"
	"github.com/quic-go/quic-go"
)

// OpType indicates inter-query operation type.
type OpType byte

const (
	OpGet OpType = iota + 1
	OpPut
	OpDelete
)

// ErrNoHandler is returned when the owner node has no DB registered for the
// requested namespace. It is deliberately distinct from storage.ErrNotFound:
// "I am not serving that namespace" must never be mistaken for "that document
// does not exist". Conflating the two once caused a production incident where
// roughly half of all reads reported missing documents while the data was
// intact.
var ErrNoHandler = errors.New("jaydb cluster: no DB handler registered for namespace")

// ErrIncompleteResponse is returned when a peer answers with neither an object
// nor an error. That is a protocol violation, not an empty result.
var ErrIncompleteResponse = errors.New("jaydb cluster: peer returned neither an object nor an error")

// ErrUnauthenticated is returned when a request fails cluster secret verification.
var ErrUnauthenticated = errors.New("jaydb cluster: request authentication failed")

// authSkew bounds how far a request timestamp may drift from the receiver's
// clock. It caps the replay window for a captured, correctly-signed request.
const authSkew = 30 * time.Second

// InterQueryReq is sent over a QUIC stream to request an operation on the key owner node.
type InterQueryReq struct {
	// Namespace selects which registered DB serves this request. A single
	// process commonly hosts many namespaces (one per tenant database), so the
	// key alone is ambiguous. Empty means "the legacy single handler", which
	// keeps embedded single-database users working unchanged.
	Namespace    string `json:"namespace,omitempty"`
	Op           OpType `json:"op"`
	Key          string `json:"key"`
	Value        []byte `json:"value,omitempty"`
	ExpectedETag string `json:"expected_etag,omitempty"`

	// TS and Auth carry request authentication when a ClusterSecret is
	// configured. Auth is an HMAC over the request's canonical form, so a peer
	// cannot be induced to read or delete arbitrary keys by anything that
	// merely reaches the mesh port.
	TS   int64  `json:"ts,omitempty"`
	Auth string `json:"auth,omitempty"`
}

// signingString is the canonical form covered by Auth. Value is hashed rather
// than embedded so signing cost stays constant for large documents.
func (r InterQueryReq) signingString() string {
	sum := sha256.Sum256(r.Value)
	return fmt.Sprintf("v1\n%d\n%s\n%d\n%s\n%s\n%s",
		r.TS, r.Namespace, r.Op, r.Key, r.ExpectedETag, hex.EncodeToString(sum[:]))
}

func computeAuth(secret string, req InterQueryReq) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(req.signingString()))
	return hex.EncodeToString(mac.Sum(nil))
}

// InterQueryResp is returned over a QUIC stream from the key owner node.
//
// Contract: exactly one of Object or Err is always set. A response with
// neither is a protocol violation and callers must surface it as an error
// rather than as an absent document.
type InterQueryResp struct {
	Object *storage.Object `json:"object,omitempty"`
	Err    string          `json:"err,omitempty"`
}

// Handler handles localized DB operations on the owner node.
type Handler interface {
	GetRaw(ctx context.Context, key string) (*storage.Object, error)
	PutRaw(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error)
	DeleteRaw(ctx context.Context, key string, expectedETag string) error
}

// MemberInfo provides a snapshot of a cluster node discovered via memberlist.
type MemberInfo struct {
	Name       string `json:"name"`
	Addr       string `json:"addr"`
	GossipPort int    `json:"gossip_port"`
	QuicPort   int    `json:"quic_port"`
	QuicAddr   string `json:"quic_addr"`
}

// NodeConfig specifies memberlist and QUIC mesh parameters.
//
// BindPort and QuicPort may each be 0 to request an OS-assigned ephemeral port.
// The kernel then guarantees a free port, which removes the "bind: address
// already in use" class of failure entirely. Because the caller cannot know the
// assigned value up front, read it back after NewNode via Node.BindPort,
// Node.QuicPort, or Node.SelfQuicAddr — those report what was actually bound.
type NodeConfig struct {
	NodeName  string
	BindAddr  string
	BindPort  int
	QuicPort  int
	JoinAddrs []string
	Ring      *sharding.Ring

	// PoolSize specifies the number of pre-opened QUIC connections to maintain per peer.
	// Defaults to 2 if <= 0.
	PoolSize int

	// DBHandler is the legacy single-database handler, used only for requests
	// that carry no namespace. Multi-namespace deployments call
	// Node.RegisterHandler instead.
	DBHandler Handler

	// ClusterSecret authenticates inter-query requests. The inter-query
	// protocol is raw Get/Put/DeleteRaw over the whole keyspace, so without a
	// secret anything that can reach the mesh port can read or delete any
	// document. Leave empty only for single-tenant local development.
	ClusterSecret string

	// DialTimeout bounds establishing a QUIC connection to a peer. Without it
	// a dial is bounded only by MaxIdleTimeout (30s), which turns one
	// unreachable peer into 30s request stalls. Default defaultDialTimeout.
	DialTimeout time.Duration

	// StreamOpenTimeout bounds opening a stream on an already-cached
	// connection. A peer that vanished leaves a conn that looks alive, and
	// OpenStreamSync on it parks until the idle timeout expires.
	// Default defaultStreamOpenTimeout.
	StreamOpenTimeout time.Duration

	// RequestTimeout bounds how long the owner node spends serving one
	// inter-query before abandoning it. Default defaultRequestTimeout.
	RequestTimeout time.Duration
}

const (
	defaultDialTimeout       = 3 * time.Second
	defaultStreamOpenTimeout = 100 * time.Millisecond
	defaultRequestTimeout    = 10 * time.Second
)

func (c NodeConfig) dialTimeout() time.Duration {
	if c.DialTimeout > 0 {
		return c.DialTimeout
	}
	return defaultDialTimeout
}

func (c NodeConfig) streamOpenTimeout() time.Duration {
	if c.StreamOpenTimeout > 0 {
		return c.StreamOpenTimeout
	}
	return defaultStreamOpenTimeout
}

func (c NodeConfig) requestTimeout() time.Duration {
	if c.RequestTimeout > 0 {
		return c.RequestTimeout
	}
	return defaultRequestTimeout
}

// Node manages memberlist cluster membership and QUIC inter-query connection mesh.
type Node struct {
	cfg       NodeConfig
	mlist     *memberlist.Memberlist
	quicLn    *quic.Listener
	ring      *sharding.Ring
	meshPool  *MeshPool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup // Tracks background goroutines for graceful shutdown
	closeOnce sync.Once      // Guarantees Close runs exactly once (memberlist.Leave panics after Shutdown)

	// Ports actually bound, resolved from the listeners after they start. These
	// differ from cfg.BindPort/cfg.QuicPort whenever a port of 0 was requested,
	// so every address JayDB advertises (node meta, ring registration,
	// SelfQuicAddr) must be built from these and never from cfg.
	bindPort int
	quicPort int

	// handlers maps namespace -> local DB. One process serves many namespaces,
	// so the owner node must be told which one a request refers to.
	handlersMu sync.RWMutex
	handlers   map[string]Handler
}

// RegisterHandler registers the local DB that serves namespace. Safe to call
// concurrently and after the node is running; db.Open calls it automatically
// when both a Ring and a ClusterNode are configured.
func (n *Node) RegisterHandler(namespace string, h Handler) {
	n.handlersMu.Lock()
	defer n.handlersMu.Unlock()
	if n.handlers == nil {
		n.handlers = make(map[string]Handler)
	}
	n.handlers[namespace] = h
}

// UnregisterHandler removes a namespace's handler, so a closed DB stops
// receiving forwarded requests.
func (n *Node) UnregisterHandler(namespace string) {
	n.handlersMu.Lock()
	defer n.handlersMu.Unlock()
	delete(n.handlers, namespace)
}

// handlerFor resolves the handler serving namespace. An empty namespace maps to
// the legacy single DBHandler so embedded single-database callers keep working.
func (n *Node) handlerFor(namespace string) (Handler, bool) {
	if namespace == "" {
		if n.cfg.DBHandler != nil {
			return n.cfg.DBHandler, true
		}
		return nil, false
	}

	n.handlersMu.RLock()
	h, ok := n.handlers[namespace]
	n.handlersMu.RUnlock()
	if ok {
		return h, true
	}

	// Fall back to the legacy handler so a single-DB deployment that started
	// passing a namespace does not break.
	if n.cfg.DBHandler != nil {
		return n.cfg.DBHandler, true
	}
	return nil, false
}

type eventDelegate struct {
	node *Node
}

func (ed *eventDelegate) NotifyJoin(n *memberlist.Node) {
	if ed.node != nil {
		quicAddr := fmt.Sprintf("%s:%d", n.Addr.String(), getQuicPort(n))
		if ed.node.ring != nil {
			ed.node.ring.AddNode(quicAddr)
		}
		if ed.node.meshPool != nil && quicAddr != ed.node.SelfQuicAddr() {
			ed.node.meshPool.AddPeer(quicAddr)
		}
	}
}

func (ed *eventDelegate) NotifyLeave(n *memberlist.Node) {
	if ed.node != nil {
		quicAddr := fmt.Sprintf("%s:%d", n.Addr.String(), getQuicPort(n))
		if ed.node.ring != nil {
			ed.node.ring.RemoveNode(quicAddr)
		}
		if ed.node.meshPool != nil {
			ed.node.meshPool.RemovePeer(quicAddr)
		}
	}
}

func (ed *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	if ed.node != nil {
		quicAddr := fmt.Sprintf("%s:%d", n.Addr.String(), getQuicPort(n))
		if ed.node.ring != nil {
			ed.node.ring.AddNode(quicAddr)
		}
		if ed.node.meshPool != nil && quicAddr != ed.node.SelfQuicAddr() {
			ed.node.meshPool.AddPeer(quicAddr)
		}
	}
}

func getQuicPort(n *memberlist.Node) int {
	if len(n.Meta) >= 2 {
		return int(binary.BigEndian.Uint16(n.Meta))
	}
	return int(n.Port) + 1000 // Fallback
}

// NewNode initializes memberlist discovery and QUIC listener mesh.
func NewNode(cfg NodeConfig) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	ring := cfg.Ring
	if ring == nil {
		ring = sharding.NewRing(1, 2)
	}

	n := &Node{
		cfg:      cfg,
		ring:     ring,
		ctx:      ctx,
		cancel:   cancel,
		handlers: make(map[string]Handler),
	}

	// 1. Setup QUIC Listener
	serverTLSConf, err := generateTLSConfig()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("quic tls config error: %w", err)
	}

	quicAddr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.QuicPort)
	quicLn, err := quic.ListenAddr(quicAddr, serverTLSConf, &quic.Config{
		MaxIncomingStreams: 2000,
		MaxIdleTimeout:     30 * time.Second,
		KeepAlivePeriod:    10 * time.Second,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("quic listen error on %s: %w", quicAddr, err)
	}
	n.quicLn = quicLn

	// Resolve the port the kernel actually assigned. With cfg.QuicPort == 0 this
	// is the ephemeral port; otherwise it echoes the requested one.
	n.quicPort, err = portFromAddr(quicLn.Addr())
	if err != nil {
		_ = quicLn.Close()
		cancel()
		return nil, fmt.Errorf("quic listen: resolve bound port: %w", err)
	}

	// Setup Client TLS Config for outbound mesh pool dials
	clientTLSConf := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // authenticated at request layer
		NextProtos:         []string{"jaydb-quic"},
		MinVersion:         tls.VersionTLS13,
	}

	n.meshPool = NewMeshPool(ctx, cfg.PoolSize, clientTLSConf, cfg.dialTimeout(), cfg.streamOpenTimeout(), n)

	n.wg.Add(1) // Track acceptLoop goroutine
	go n.acceptLoop()

	// Register self in ring
	n.ring.AddNode(n.SelfQuicAddr())

	// 2. Setup Memberlist
	mlConfig := memberlist.DefaultLANConfig()
	mlConfig.Name = cfg.NodeName
	mlConfig.BindAddr = cfg.BindAddr
	mlConfig.BindPort = cfg.BindPort
	mlConfig.Events = &eventDelegate{node: n}

	// Advertise the resolved QUIC port so peers can reach this node's mesh
	// listener. Using cfg.QuicPort here would publish 0 for ephemeral binds.
	meta := make([]byte, 2)
	binary.BigEndian.PutUint16(meta, uint16(n.quicPort))
	mlConfig.Delegate = &nodeDelegate{meta: meta}

	mlist, err := memberlist.Create(mlConfig)
	if err != nil {
		_ = quicLn.Close()
		n.meshPool.Close()
		cancel()
		return nil, fmt.Errorf("memberlist create error: %w", err)
	}
	n.mlist = mlist
	n.bindPort = int(mlist.LocalNode().Port)

	if len(cfg.JoinAddrs) > 0 {
		_, _ = mlist.Join(cfg.JoinAddrs)
	}

	// Sync initial memberlist members into ring and mesh connection pool
	for _, member := range mlist.Members() {
		mQuicAddr := fmt.Sprintf("%s:%d", member.Addr.String(), getQuicPort(member))
		n.ring.AddNode(mQuicAddr)
		if mQuicAddr != n.SelfQuicAddr() {
			n.meshPool.AddPeer(mQuicAddr)
		}
	}

	return n, nil
}

// portFromAddr extracts the port from a listener address, tolerating any
// net.Addr implementation rather than assuming *net.UDPAddr.
func portFromAddr(addr net.Addr) (int, error) {
	if addr == nil {
		return 0, fmt.Errorf("nil listener address")
	}
	if udp, ok := addr.(*net.UDPAddr); ok {
		return udp.Port, nil
	}

	_, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", addr.String(), err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("parse port %q: %w", portStr, err)
	}
	return port, nil
}

type nodeDelegate struct {
	meta []byte
}

func (d *nodeDelegate) NodeMeta(limit int) []byte {
	return d.meta
}

func (d *nodeDelegate) NotifyMsg(b []byte)                         {}
func (d *nodeDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *nodeDelegate) LocalState(join bool) []byte                { return nil }
func (d *nodeDelegate) MergeRemoteState(buf []byte, join bool)     {}

func (n *Node) acceptLoop() {
	defer n.wg.Done() // Mark acceptLoop as finished
	for {
		conn, err := n.quicLn.Accept(n.ctx)
		if err != nil {
			select {
			case <-n.ctx.Done():
				return
			default:
				continue
			}
		}
		n.wg.Add(1) // Track each handleConn goroutine
		go n.handleConn(conn)
	}
}

func (n *Node) handleConn(conn *quic.Conn) {
	defer n.wg.Done() // Mark handleConn as finished
	// Limit concurrent streams per connection to prevent goroutine explosion
	const maxConcurrentStreams = 1000
	streamSem := make(chan struct{}, maxConcurrentStreams)

	for {
		stream, err := conn.AcceptStream(n.ctx)
		if err != nil {
			return
		}

		// Acquire semaphore slot (blocks if at limit)
		select {
		case streamSem <- struct{}{}:
			go func(s *quic.Stream) {
				defer func() { <-streamSem }() // Release slot when done
				n.handleStream(s)
			}(stream)
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) handleStream(stream *quic.Stream) {
	defer (*stream).Close()

	decoder := json.NewDecoder(stream)
	var req InterQueryReq
	if err := decoder.Decode(&req); err != nil {
		return
	}

	resp := n.serve(req)
	_ = json.NewEncoder(stream).Encode(&resp)
}

// serve executes one inter-query request and ALWAYS returns a response with
// exactly one of Object or Err populated. Returning an empty response on an
// unserviceable request is what previously made a misconfigured mesh
// indistinguishable from missing data.
func (n *Node) serve(req InterQueryReq) InterQueryResp {
	if err := n.authenticate(req); err != nil {
		return InterQueryResp{Err: err.Error()}
	}

	handler, ok := n.handlerFor(req.Namespace)
	if !ok {
		// Fail loud. The caller must be able to tell this apart from a genuinely
		// absent document.
		return InterQueryResp{Err: fmt.Sprintf("%s %q", ErrNoHandler.Error(), req.Namespace)}
	}

	// Bound the work: without a deadline the owner could hold a stream open for
	// as long as its storage backend takes to answer.
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.requestTimeout())
	defer cancel()

	switch req.Op {
	case OpGet:
		obj, err := handler.GetRaw(ctx, req.Key)
		if err != nil {
			return InterQueryResp{Err: err.Error()}
		}
		if obj == nil {
			// A nil object with no error would serialize to an empty response.
			return InterQueryResp{Err: storage.ErrNotFound.Error()}
		}
		return InterQueryResp{Object: obj}

	case OpPut:
		obj, err := handler.PutRaw(ctx, req.Key, req.Value, req.ExpectedETag)
		if err != nil {
			return InterQueryResp{Err: err.Error()}
		}
		if obj == nil {
			return InterQueryResp{Err: "jaydb cluster: put returned no object"}
		}
		return InterQueryResp{Object: obj}

	case OpDelete:
		if err := handler.DeleteRaw(ctx, req.Key, req.ExpectedETag); err != nil {
			return InterQueryResp{Err: err.Error()}
		}
		// Delete has no object to return; an empty Err means success. This is
		// the one operation where both fields are legitimately empty, and the
		// client only inspects Object for Get/Put.
		return InterQueryResp{}

	default:
		return InterQueryResp{Err: fmt.Sprintf("jaydb cluster: unknown op %d", req.Op)}
	}
}

// authenticate verifies the request HMAC when a cluster secret is configured.
func (n *Node) authenticate(req InterQueryReq) error {
	if n.cfg.ClusterSecret == "" {
		return nil
	}

	if req.Auth == "" {
		return ErrUnauthenticated
	}
	// Reject stale requests so a captured signed request cannot be replayed
	// indefinitely.
	skew := time.Since(time.Unix(req.TS, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > authSkew {
		return ErrUnauthenticated
	}

	want := computeAuth(n.cfg.ClusterSecret, req)
	if subtle.ConstantTimeCompare([]byte(want), []byte(req.Auth)) != 1 {
		return ErrUnauthenticated
	}
	return nil
}

// ExecuteInterQuery routes an inter-query operation to targetNode over the pre-opened QUIC mesh pool.
func (n *Node) ExecuteInterQuery(ctx context.Context, targetNode string, req InterQueryReq) (*InterQueryResp, error) {
	if n.cfg.ClusterSecret != "" {
		req.TS = time.Now().Unix()
		req.Auth = computeAuth(n.cfg.ClusterSecret, req)
	}

	resp, err := n.roundTrip(ctx, targetNode, req)
	if err == nil {
		return resp, nil
	}
	// One retry: if connection dropped or stale, retry on another pooled connection if ctx not expired
	if ctx.Err() != nil {
		return nil, err
	}
	return n.roundTrip(ctx, targetNode, req)
}

func (n *Node) roundTrip(ctx context.Context, targetNode string, req InterQueryReq) (*InterQueryResp, error) {
	if n.meshPool == nil {
		return nil, errors.New("jaydb cluster: mesh pool not initialized")
	}

	stream, dropFunc, err := n.meshPool.GetStream(ctx, targetNode)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	if err := json.NewEncoder(stream).Encode(&req); err != nil {
		if dropFunc != nil {
			dropFunc()
		}
		return nil, err
	}

	var resp InterQueryResp
	if err := json.NewDecoder(stream).Decode(&resp); err != nil {
		if dropFunc != nil {
			dropFunc()
		}
		return nil, err
	}

	return &resp, nil
}

// SelfQuicAddr returns the node's QUIC address string, using the port actually
// bound rather than the requested one, so it stays correct for ephemeral binds.
func (n *Node) SelfQuicAddr() string {
	return fmt.Sprintf("%s:%d", n.cfg.BindAddr, n.quicPort)
}

// QuicPort returns the QUIC mesh port actually bound. Call this after NewNode to
// discover the assigned port when NodeConfig.QuicPort was 0.
func (n *Node) QuicPort() int {
	return n.quicPort
}

// BindPort returns the memberlist gossip port actually bound. Call this after
// NewNode to discover the assigned port when NodeConfig.BindPort was 0; the
// result is what peers should be given as a JoinAddrs entry.
func (n *Node) BindPort() int {
	return n.bindPort
}

// GossipAddr returns the host:port other nodes should use to join this node.
func (n *Node) GossipAddr() string {
	return fmt.Sprintf("%s:%d", n.cfg.BindAddr, n.bindPort)
}

// Members returns the list of active cluster members discovered via memberlist.
func (n *Node) Members() []MemberInfo {
	if n.mlist == nil {
		return nil
	}
	mlMembers := n.mlist.Members()
	members := make([]MemberInfo, len(mlMembers))
	for i, m := range mlMembers {
		qPort := getQuicPort(m)
		members[i] = MemberInfo{
			Name:       m.Name,
			Addr:       m.Addr.String(),
			GossipPort: int(m.Port),
			QuicPort:   qPort,
			QuicAddr:   fmt.Sprintf("%s:%d", m.Addr.String(), qPort),
		}
	}
	return members
}

// MemberCount returns the total number of members currently in the cluster.
func (n *Node) MemberCount() int {
	if n.mlist == nil {
		if n.ring != nil {
			return n.ring.NodeCount()
		}
		return 0
	}
	return n.mlist.NumMembers()
}

// Ring returns the deterministic sharding ring associated with this node.
func (n *Node) Ring() *sharding.Ring {
	return n.ring
}

// IsOwner returns whether this node is the designated shard owner for the given key.
func (n *Node) IsOwner(key string) bool {
	if n.ring == nil {
		return true
	}
	return n.ring.IsOwner(key, n.SelfQuicAddr())
}

// MeshPool returns the underlying MeshPool.
func (n *Node) MeshPool() *MeshPool {
	return n.meshPool
}

// Close gracefully terminates node connection pool and memberlist.
//
// Close is idempotent and safe to call concurrently: memberlist panics if Leave
// is invoked after Shutdown, and a Node is routinely closed twice in practice
// because db.Close also closes the ClusterNode it was configured with (so a
// caller holding both a Node and a DB has no single correct shutdown order).
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		// Signal shutdown to all goroutines
		n.cancel()

		// Leave memberlist gracefully
		if n.mlist != nil {
			_ = n.mlist.Leave(2 * time.Second)
			_ = n.mlist.Shutdown()
		}

		// Close QUIC listener (stops accepting new connections)
		if n.quicLn != nil {
			_ = n.quicLn.Close()
		}

		// Close proactive mesh connection pool
		if n.meshPool != nil {
			n.meshPool.Close()
		}

		// Wait for all background goroutines to finish (acceptLoop and handleConn goroutines)
		n.wg.Wait()
	})

	return nil
}

func generateTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("0.0.0.0")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyBytes := x509.MarshalPKCS1PrivateKey(key)
	certPEM := pemEncode("CERTIFICATE", certDER)
	keyPEM := pemEncode("RSA PRIVATE KEY", keyBytes)

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"jaydb-quic"},
	}, nil
}

func pemEncode(certType string, bytes []byte) []byte {
	return []byte(fmt.Sprintf("-----BEGIN %s-----\n%s\n-----END %s-----\n", certType, base64Encode(bytes), certType))
}

func base64Encode(src []byte) string {
	const encodeStd = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	buf := make([]byte, ((len(src)+2)/3)*4)
	di, si := 0, 0
	n := (len(src) / 3) * 3
	for si < n {
		val := uint32(src[si])<<16 | uint32(src[si+1])<<8 | uint32(src[si+2])
		buf[di] = encodeStd[val>>18&0x3F]
		buf[di+1] = encodeStd[val>>12&0x3F]
		buf[di+2] = encodeStd[val>>6&0x3F]
		buf[di+3] = encodeStd[val&0x3F]
		si += 3
		di += 4
	}
	if remain := len(src) - si; remain == 1 {
		val := uint32(src[si]) << 16
		buf[di] = encodeStd[val>>18&0x3F]
		buf[di+1] = encodeStd[val>>12&0x3F]
		buf[di+2] = '='
		buf[di+3] = '='
	} else if remain == 2 {
		val := uint32(src[si])<<16 | uint32(src[si+1])<<8
		buf[di] = encodeStd[val>>18&0x3F]
		buf[di+1] = encodeStd[val>>12&0x3F]
		buf[di+2] = encodeStd[val>>6&0x3F]
		buf[di+3] = '='
	}
	// Add line breaks every 64 chars
	var out []byte
	for i := 0; i < len(buf); i += 64 {
		end := i + 64
		if end > len(buf) {
			end = len(buf)
		}
		out = append(out, buf[i:end]...)
		out = append(out, '\n')
	}
	return string(out)
}
