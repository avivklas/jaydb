package cluster

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
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

// InterQueryReq is sent over a QUIC stream to request an operation on the key owner node.
type InterQueryReq struct {
	Op           OpType `json:"op"`
	Key          string `json:"key"`
	Value        []byte `json:"value,omitempty"`
	ExpectedETag string `json:"expected_etag,omitempty"`
}

// InterQueryResp is returned over a QUIC stream from the key owner node.
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

// NodeConfig specifies memberlist and QUIC mesh parameters.
type NodeConfig struct {
	NodeName  string
	BindAddr  string
	BindPort  int
	QuicPort  int
	JoinAddrs []string
	Ring      *sharding.Ring
	DBHandler Handler
}

// Node manages memberlist cluster membership and QUIC inter-query connection mesh.
type Node struct {
	cfg       NodeConfig
	mlist     *memberlist.Memberlist
	quicLn    *quic.Listener
	ring      *sharding.Ring
	mu        sync.RWMutex
	conns     map[string]*quic.Conn
	ctx       context.Context
	cancel    context.CancelFunc
}

type eventDelegate struct {
	node *Node
}

func (ed *eventDelegate) NotifyJoin(n *memberlist.Node) {
	if ed.node != nil && ed.node.ring != nil {
		quicAddr := fmt.Sprintf("%s:%d", n.Addr.String(), getQuicPort(n))
		ed.node.ring.AddNode(quicAddr)
	}
}

func (ed *eventDelegate) NotifyLeave(n *memberlist.Node) {
	if ed.node != nil && ed.node.ring != nil {
		quicAddr := fmt.Sprintf("%s:%d", n.Addr.String(), getQuicPort(n))
		ed.node.ring.RemoveNode(quicAddr)
	}
}

func (ed *eventDelegate) NotifyUpdate(n *memberlist.Node) {}

func getQuicPort(n *memberlist.Node) int {
	if len(n.Meta) >= 2 {
		return int(binary.BigEndian.Uint16(n.Meta))
	}
	return int(n.Port) + 1000 // Fallback
}

// NewNode initializes memberlist discovery and QUIC listener mesh.
func NewNode(cfg NodeConfig) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		cfg:    cfg,
		ring:   cfg.Ring,
		conns:  make(map[string]*quic.Conn),
		ctx:    ctx,
		cancel: cancel,
	}

	// 1. Setup QUIC Listener
	tlsConf, err := generateTLSConfig()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("quic tls config error: %w", err)
	}

	quicAddr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.QuicPort)
	quicLn, err := quic.ListenAddr(quicAddr, tlsConf, &quic.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 10 * time.Second,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("quic listen error on %s: %w", quicAddr, err)
	}
	n.quicLn = quicLn

	go n.acceptLoop()

	// 2. Setup Memberlist
	mlConfig := memberlist.DefaultLANConfig()
	mlConfig.Name = cfg.NodeName
	mlConfig.BindAddr = cfg.BindAddr
	mlConfig.BindPort = cfg.BindPort
	mlConfig.Events = &eventDelegate{node: n}

	meta := make([]byte, 2)
	binary.BigEndian.PutUint16(meta, uint16(cfg.QuicPort))
	mlConfig.Delegate = &nodeDelegate{meta: meta}

	mlist, err := memberlist.Create(mlConfig)
	if err != nil {
		_ = quicLn.Close()
		cancel()
		return nil, fmt.Errorf("memberlist create error: %w", err)
	}
	n.mlist = mlist

	// Register self in ring
	if n.ring != nil {
		selfQuic := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.QuicPort)
		n.ring.AddNode(selfQuic)
	}

	if len(cfg.JoinAddrs) > 0 {
		_, _ = mlist.Join(cfg.JoinAddrs)
	}

	return n, nil
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
		go n.handleConn(conn)
	}
}

func (n *Node) handleConn(conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(n.ctx)
		if err != nil {
			return
		}
		go n.handleStream(stream)
	}
}

func (n *Node) handleStream(stream *quic.Stream) {
	defer stream.Close()

	decoder := json.NewDecoder(stream)
	var req InterQueryReq
	if err := decoder.Decode(&req); err != nil {
		return
	}

	var resp InterQueryResp
	if n.cfg.DBHandler != nil {
		switch req.Op {
		case OpGet:
			obj, err := n.cfg.DBHandler.GetRaw(n.ctx, req.Key)
			if err != nil {
				resp.Err = err.Error()
			} else {
				resp.Object = obj
			}
		case OpPut:
			obj, err := n.cfg.DBHandler.PutRaw(n.ctx, req.Key, req.Value, req.ExpectedETag)
			if err != nil {
				resp.Err = err.Error()
			} else {
				resp.Object = obj
			}
		case OpDelete:
			err := n.cfg.DBHandler.DeleteRaw(n.ctx, req.Key, req.ExpectedETag)
			if err != nil {
				resp.Err = err.Error()
			}
		}
	}

	_ = json.NewEncoder(stream).Encode(&resp)
}

func (n *Node) getConn(targetNode string) (*quic.Conn, error) {
	n.mu.RLock()
	conn, ok := n.conns[targetNode]
	n.mu.RUnlock()
	if ok {
		return conn, nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if conn, ok := n.conns[targetNode]; ok {
		return conn, nil
	}

	tlsConf := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"jaydb-quic"}}
	conn, err := quic.DialAddr(n.ctx, targetNode, tlsConf, &quic.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("quic dial error to %s: %w", targetNode, err)
	}

	n.conns[targetNode] = conn
	return conn, nil
}

// ExecuteInterQuery routes an inter-query operation to targetNode over QUIC mesh.
func (n *Node) ExecuteInterQuery(ctx context.Context, targetNode string, req InterQueryReq) (*InterQueryResp, error) {
	conn, err := n.getConn(targetNode)
	if err != nil {
		return nil, err
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		// Close stale conn and retry once
		n.mu.Lock()
		delete(n.conns, targetNode)
		n.mu.Unlock()

		conn, err = n.getConn(targetNode)
		if err != nil {
			return nil, err
		}
		stream, err = conn.OpenStreamSync(ctx)
		if err != nil {
			return nil, err
		}
	}
	defer stream.Close()

	if err := json.NewEncoder(stream).Encode(&req); err != nil {
		return nil, err
	}

	var resp InterQueryResp
	if err := json.NewDecoder(stream).Decode(&resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// SelfQuicAddr returns the node's QUIC address string.
func (n *Node) SelfQuicAddr() string {
	return fmt.Sprintf("%s:%d", n.cfg.BindAddr, n.cfg.QuicPort)
}

// Close gracefully terminates node connection pool and memberlist.
func (n *Node) Close() error {
	n.cancel()
	if n.mlist != nil {
		_ = n.mlist.Leave(2 * time.Second)
		_ = n.mlist.Shutdown()
	}
	if n.quicLn != nil {
		_ = n.quicLn.Close()
	}
	n.mu.Lock()
	for _, conn := range n.conns {
		_ = conn.CloseWithError(0, "node closing")
	}
	n.mu.Unlock()
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
