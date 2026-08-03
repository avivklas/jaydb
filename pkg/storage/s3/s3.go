package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/avivklas/jaydb/pkg/storage"
)

// Config defines connection details for an S3 / S3-compatible bucket endpoint.
type Config struct {
	Endpoint   string // e.g. "https://s3.amazonaws.com" or "http://localhost:9000"
	Bucket     string
	Region     string
	AccessKey  string
	SecretKey  string
	HTTPClient *http.Client
}

// Driver implements storage.Driver using S3 HTTP REST endpoints with conditional PUT/GET.
type Driver struct {
	cfg        Config
	client     *http.Client
	baseURL    string
	mu         sync.RWMutex
}

// NewDriver initializes a new S3 cold storage driver.
func NewDriver(cfg Config) (storage.Driver, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "https://s3.amazonaws.com"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	endpoint := strings.TrimSuffix(cfg.Endpoint, "/")
	baseURL := fmt.Sprintf("%s/%s", endpoint, cfg.Bucket)

	return &Driver{
		cfg:     cfg,
		client:  cfg.HTTPClient,
		baseURL: baseURL,
	}, nil
}

func (d *Driver) objectURL(key string) string {
	escapedKey := url.PathEscape(strings.TrimPrefix(key, "/"))
	return fmt.Sprintf("%s/%s", d.baseURL, escapedKey)
}

func (d *Driver) Get(ctx context.Context, key string) (*storage.Object, error) {
	u := d.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("s3 get request create error: %w", err)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 get request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, storage.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("s3 get failed (%d): %s", resp.StatusCode, string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 read body error: %w", err)
	}

	etag := strings.Trim(resp.Header.Get("ETag"), `"`)
	etag = fmt.Sprintf(`"%s"`, etag)

	modTime, _ := time.Parse(http.TimeFormat, resp.Header.Get("Last-Modified"))

	return &storage.Object{
		Key:     key,
		Value:   data,
		ETag:    etag,
		ModTime: modTime,
	}, nil
}

func (d *Driver) Put(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	u := d.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(value))
	if err != nil {
		return nil, fmt.Errorf("s3 put request create error: %w", err)
	}

	if expectedETag != "" {
		if expectedETag == storage.MatchAnyETag {
			req.Header.Set("If-None-Match", "*")
		} else {
			req.Header.Set("If-Match", expectedETag)
		}
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 put request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusPreconditionFailed {
		if expectedETag == storage.MatchAnyETag {
			return nil, storage.ErrAlreadyExists
		}
		return nil, storage.ErrVersionMismatch
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("s3 put failed (%d): %s", resp.StatusCode, string(body))
	}

	newETag := strings.Trim(resp.Header.Get("ETag"), `"`)
	if newETag != "" {
		newETag = fmt.Sprintf(`"%s"`, newETag)
	} else {
		newETag = fmt.Sprintf(`"%x"`, time.Now().UnixNano())
	}

	return &storage.Object{
		Key:     key,
		Value:   append([]byte(nil), value...),
		ETag:    newETag,
		ModTime: time.Now(),
	}, nil
}

func (d *Driver) Delete(ctx context.Context, key string, expectedETag string) error {
	u := d.objectURL(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return fmt.Errorf("s3 delete request create error: %w", err)
	}

	if expectedETag != "" && expectedETag != storage.MatchAnyETag {
		req.Header.Set("If-Match", expectedETag)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("s3 delete request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusPreconditionFailed {
		return storage.ErrVersionMismatch
	}
	if resp.StatusCode == http.StatusNotFound {
		return storage.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("s3 delete failed (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}

type listBucketResult struct {
	XMLName        xml.Name `xml:"ListBucketResult"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	IsTruncated    bool     `xml:"IsTruncated"`
	Contents       []struct {
		Key          string    `xml:"Key"`
		Size         int64     `xml:"Size"`
		ETag         string    `xml:"ETag"`
		LastModified time.Time `xml:"LastModified"`
	} `xml:"Contents"`
}

func (d *Driver) List(ctx context.Context, prefix string, opts storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	u, err := url.Parse(d.baseURL)
	if err != nil {
		return nil, "", fmt.Errorf("s3 list invalid url: %w", err)
	}

	q := u.Query()
	q.Set("list-type", "2")
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	if opts.Limit > 0 {
		q.Set("max-keys", strconv.Itoa(opts.Limit))
	}
	if opts.Cursor != "" {
		q.Set("continuation-token", opts.Cursor)
	}
	if opts.Delimiter != "" {
		q.Set("delimiter", opts.Delimiter)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, "", fmt.Errorf("s3 list request create error: %w", err)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("s3 list request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("s3 list failed (%d): %s", resp.StatusCode, string(body))
	}

	var res listBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, "", fmt.Errorf("s3 decode list xml error: %w", err)
	}

	var metas []*storage.KeyMeta
	for _, c := range res.Contents {
		etag := fmt.Sprintf(`"%s"`, strings.Trim(c.ETag, `"`))
		metas = append(metas, &storage.KeyMeta{
			Key:     c.Key,
			Size:    c.Size,
			ETag:    etag,
			ModTime: c.LastModified,
		})
	}

	nextCursor := ""
	if res.IsTruncated {
		nextCursor = res.NextContinuationToken
	}

	return metas, nextCursor, nil
}

func (d *Driver) Close() error {
	return nil
}
