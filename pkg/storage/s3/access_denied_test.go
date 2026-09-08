package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/avivklas/jaydb/pkg/storage"
)

// denyingTransport answers every PUT with a genuine S3 AccessDenied envelope, so
// the SDK surfaces ErrorCode() == "AccessDenied" the way a real denial does.
type denyingTransport struct{}

func (denyingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodPut {
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    r,
		}, nil
	}

	body := `<?xml version="1.0" encoding="UTF-8"?>` +
		`<Error><Code>AccessDenied</Code><Message>Access Denied</Message>` +
		`<RequestId>TEST</RequestId><HostId>TEST</HostId></Error>`

	h := make(http.Header)
	h.Set("Content-Type", "application/xml")

	return &http.Response{
		StatusCode: http.StatusForbidden,
		Header:     h,
		Body:       io.NopCloser(bytes.NewReader([]byte(body))),
		Request:    r,
	}, nil
}

func newDenyingDriver(t *testing.T) storage.Driver {
	t.Helper()

	drv, err := NewDriver(Config{
		Endpoint:   "http://s3.local",
		Bucket:     "testbucket",
		HTTPClient: &http.Client{Transport: denyingTransport{}},
	})
	if err != nil {
		t.Fatalf("failed to create s3 driver: %v", err)
	}

	return drv
}

// A denied conditional overwrite must NOT be reported as a CAS mismatch.
//
// An If-Match write needs both s3:PutObject and s3:GetObject. When the second is
// missing, every attempt is denied -- permanently. Returning ErrVersionMismatch
// turns that into a 412 at the API edge, which is exactly what a losing
// concurrent write looks like, so the caller re-reads, gets a validator that was
// never stale, sends it back, and is denied again. The misconfiguration is
// invisible and the retry loop cannot terminate.
func TestPutIfMatchAccessDeniedIsNotVersionMismatch(t *testing.T) {
	drv := newDenyingDriver(t)

	_, err := drv.Put(context.Background(), "doc1", []byte("v2"), `"some-current-etag"`)
	if err == nil {
		t.Fatal("expected an error from a denied conditional put")
	}

	if errors.Is(err, storage.ErrVersionMismatch) {
		t.Fatal("AccessDenied was reported as ErrVersionMismatch: a permissions failure " +
			"reaches the client as a 412 CAS conflict and is retried forever")
	}
	if errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatal("AccessDenied on an If-Match write was reported as ErrAlreadyExists")
	}

	// The error has to name the missing grant, or the operator is left staring at
	// a conflict that no amount of re-reading can clear.
	if !strings.Contains(err.Error(), "s3:GetObject") {
		t.Errorf("error does not name the required permission: %v", err)
	}
}

// If-None-Match keeps the old mapping on purpose: S3 may answer AccessDenied
// instead of 412 so a caller without read access cannot use the precondition as
// an existence oracle, and for create-only both answers mean the same thing --
// this caller did not create the object.
func TestPutCreateOnlyAccessDeniedStaysAlreadyExists(t *testing.T) {
	drv := newDenyingDriver(t)

	_, err := drv.Put(context.Background(), "doc1", []byte("v1"), storage.MatchAnyETag)
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("create-only denial = %v, want ErrAlreadyExists", err)
	}
}

// An unconditional put has no precondition to misreport, so a denial must stay a
// plain failure rather than being dressed up as either CAS outcome.
func TestPutUnconditionalAccessDeniedIsPlainError(t *testing.T) {
	drv := newDenyingDriver(t)

	_, err := drv.Put(context.Background(), "doc1", []byte("v1"), "")
	if err == nil {
		t.Fatal("expected an error from a denied put")
	}
	if errors.Is(err, storage.ErrVersionMismatch) || errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("unconditional denial mapped to a precondition error: %v", err)
	}
}
