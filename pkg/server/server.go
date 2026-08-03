package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/valyala/fasthttp"
)

// Server encapsulates the FastHTTP server wrapper around the embedded DB engine.
type Server struct {
	db       db.DB
	ring     *sharding.Ring
	nodeAddr string
	client   *fasthttp.Client
	listener net.Listener
}

// Options configures the server instance.
type Options struct {
	DB       db.DB
	Ring     *sharding.Ring
	NodeAddr string
}

// NewServer initializes a new FastHTTP server wrapping the embedded DB.
func NewServer(opts Options) (*Server, error) {
	if opts.DB == nil {
		return nil, fmt.Errorf("server: embedded DB instance is required")
	}
	client := &fasthttp.Client{
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return &Server{
		db:       opts.DB,
		ring:     opts.Ring,
		nodeAddr: opts.NodeAddr,
		client:   client,
	}, nil
}

// HandleRequest is the main FastHTTP request handler.
func (s *Server) HandleRequest(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())

	if path == "/v1/health" {
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetContentType("application/json")
		_, _ = ctx.WriteString(`{"status":"ok"}`)
		return
	}

	if strings.HasPrefix(path, "/v1/kv/") {
		key := strings.TrimPrefix(path, "/v1/kv/")
		if key == "" {
			ctx.Error("key path is required", fasthttp.StatusBadRequest)
			return
		}

		// Inter-node forwarding check
		if s.ring != nil && s.nodeAddr != "" {
			targetNode := s.ring.GetNode(key)
			if targetNode != "" && targetNode != s.nodeAddr {
				s.proxyToNode(ctx, targetNode)
				return
			}
		}

		switch string(ctx.Method()) {
		case fasthttp.MethodGet:
			s.handleGet(ctx, key)
		case fasthttp.MethodPut:
			s.handlePut(ctx, key)
		case fasthttp.MethodDelete:
			s.handleDelete(ctx, key)
		default:
			ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
		}
		return
	}

	if strings.HasPrefix(path, "/v1/list/") {
		prefix := strings.TrimPrefix(path, "/v1/list/")
		s.handleList(ctx, prefix)
		return
	}

	ctx.Error("not found", fasthttp.StatusNotFound)
}

func (s *Server) proxyToNode(ctx *fasthttp.RequestCtx, targetNode string) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	ctx.Request.CopyTo(req)

	targetURL := fmt.Sprintf("http://%s%s", targetNode, string(ctx.Request.URI().FullURI()))
	req.SetRequestURI(targetURL)
	req.Header.Set("X-Forwarded-By", s.nodeAddr)

	if err := s.client.Do(req, resp); err != nil {
		ctx.Error(fmt.Sprintf("proxy to node %s error: %v", targetNode, err), fasthttp.StatusBadGateway)
		return
	}

	resp.CopyTo(&ctx.Response)
}

func (s *Server) handleGet(ctx *fasthttp.RequestCtx, key string) {
	var rawData []byte
	meta, err := s.db.Get(context.Background(), key, &rawData)
	if err != nil {
		if err == storage.ErrNotFound {
			ctx.Error("document not found", fasthttp.StatusNotFound)
			return
		}
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	ctx.Response.Header.Set("ETag", meta.ETag)
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody(rawData)
}

func (s *Server) handlePut(ctx *fasthttp.RequestCtx, key string) {
	body := ctx.PostBody()
	if len(body) == 0 {
		ctx.Error("request body is required", fasthttp.StatusBadRequest)
		return
	}

	ifMatch := string(ctx.Request.Header.Peek("If-Match"))
	ifNoneMatch := string(ctx.Request.Header.Peek("If-None-Match"))

	var putOpts []db.PutOption
	if ifNoneMatch == "*" {
		putOpts = append(putOpts, db.CreateOnly())
	} else if ifMatch != "" {
		putOpts = append(putOpts, db.WithExpectedETag(ifMatch))
	}

	meta, err := s.db.Put(context.Background(), key, body, putOpts...)
	if err != nil {
		if err == storage.ErrVersionMismatch || err == storage.ErrAlreadyExists {
			ctx.Error("CAS precondition failed: "+err.Error(), fasthttp.StatusPreconditionFailed)
			return
		}
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	ctx.Response.Header.Set("ETag", meta.ETag)
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	respBody, _ := json.Marshal(meta)
	ctx.SetBody(respBody)
}

func (s *Server) handleDelete(ctx *fasthttp.RequestCtx, key string) {
	ifMatch := string(ctx.Request.Header.Peek("If-Match"))

	var delOpts []db.DeleteOption
	if ifMatch != "" {
		delOpts = append(delOpts, db.WithDeleteExpectedETag(ifMatch))
	}

	err := s.db.Delete(context.Background(), key, delOpts...)
	if err != nil {
		if err == storage.ErrNotFound {
			ctx.Error("document not found", fasthttp.StatusNotFound)
			return
		}
		if err == storage.ErrVersionMismatch {
			ctx.Error("CAS precondition failed", fasthttp.StatusPreconditionFailed)
			return
		}
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusNoContent)
}

func (s *Server) handleList(ctx *fasthttp.RequestCtx, prefix string) {
	limit := ctx.QueryArgs().GetUintOrZero("limit")
	if limit == 0 {
		limit = 100
	}

	items, err := s.db.List(context.Background(), prefix, limit)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)

	buf := new(bytes.Buffer)
	_ = json.NewEncoder(buf).Encode(items)
	ctx.SetBody(buf.Bytes())
}

// ListenAndServe starts the FastHTTP server on addr.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = ln
	return fasthttp.Serve(ln, s.HandleRequest)
}

// Close gracefully stops the server listener.
func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
