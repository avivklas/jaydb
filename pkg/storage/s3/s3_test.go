package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/avivklas/jaydb/pkg/storage"
)

type mockTransport struct {
	storedData map[string][]byte
	storedETag map[string]string
}

func (m *mockTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	key := r.URL.Path

	wHeader := make(http.Header)
	var body []byte
	statusCode := http.StatusOK

	switch r.Method {
	case http.MethodGet:
		data, exists := m.storedData[key]
		if !exists {
			statusCode = http.StatusNotFound
			body = []byte("Not Found")
		} else {
			wHeader.Set("ETag", m.storedETag[key])
			wHeader.Set("Last-Modified", http.TimeFormat)
			body = data
		}

	case http.MethodPut:
		ifMatch := r.Header.Get("If-Match")
		ifNoneMatch := r.Header.Get("If-None-Match")

		_, exists := m.storedData[key]
		if ifNoneMatch == "*" && exists {
			statusCode = http.StatusPreconditionFailed
			body = []byte("Precondition Failed")
		} else if ifMatch != "" && (!exists || m.storedETag[key] != ifMatch) {
			statusCode = http.StatusPreconditionFailed
			body = []byte("Precondition Failed")
		} else {
			buf, _ := io.ReadAll(r.Body)
			m.storedData[key] = buf
			newETag := fmt.Sprintf(`"etag-%d"`, len(m.storedData))
			m.storedETag[key] = newETag
			wHeader.Set("ETag", newETag)
			statusCode = http.StatusOK
		}

	case http.MethodDelete:
		ifMatch := r.Header.Get("If-Match")
		if ifMatch != "" && m.storedETag[key] != ifMatch {
			statusCode = http.StatusPreconditionFailed
			body = []byte("Precondition Failed")
		} else {
			delete(m.storedData, key)
			delete(m.storedETag, key)
			statusCode = http.StatusNoContent
		}
	}

	return &http.Response{
		StatusCode: statusCode,
		Header:     wHeader,
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    r,
	}, nil
}

func TestS3Driver_CAS_Mock(t *testing.T) {
	transport := &mockTransport{
		storedData: make(map[string][]byte),
		storedETag: make(map[string]string),
	}

	client := &http.Client{Transport: transport}

	ctx := context.Background()
	driver, err := NewDriver(Config{
		Endpoint:   "http://s3.local",
		Bucket:     "testbucket",
		HTTPClient: client,
	})
	if err != nil {
		t.Fatalf("failed to create s3 driver: %v", err)
	}

	// 1. Put create (If-None-Match: *)
	obj1, err := driver.Put(ctx, "doc1", []byte("v1"), storage.MatchAnyETag)
	if err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}

	// 2. Put create again should fail with ErrAlreadyExists
	_, err = driver.Put(ctx, "doc1", []byte("v2"), storage.MatchAnyETag)
	if err != storage.ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	// 3. Update with wrong ETag
	_, err = driver.Put(ctx, "doc1", []byte("v2"), `"badetag"`)
	if err != storage.ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// 4. Update with correct ETag
	obj2, err := driver.Put(ctx, "doc1", []byte("v2"), obj1.ETag)
	if err != nil {
		t.Fatalf("unexpected update error: %v", err)
	}

	// 5. Get object
	readObj, err := driver.Get(ctx, "doc1")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if string(readObj.Value) != "v2" {
		t.Fatalf("expected 'v2', got '%s'", readObj.Value)
	}
	if readObj.ETag != obj2.ETag {
		t.Fatalf("expected ETag %s, got %s", obj2.ETag, readObj.ETag)
	}
}
