package fs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/avivklas/jaydb/pkg/storage"
)

// Driver implements a local filesystem-backed storage driver.
type Driver struct {
	mu      sync.RWMutex
	rootDir string
}

// NewDriver initializes a filesystem storage driver at rootDir.
func NewDriver(rootDir string) (storage.Driver, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("fs storage: failed to create root dir: %w", err)
	}
	return &Driver{rootDir: rootDir}, nil
}

func (d *Driver) keyToPath(key string) string {
	cleaned := filepath.Clean(key)
	return filepath.Join(d.rootDir, cleaned+".data")
}

func (d *Driver) generateETag(data []byte, modTime time.Time) string {
	h := sha256.New()
	h.Write(data)
	h.Write([]byte(fmt.Sprintf("%d", modTime.UnixNano())))
	return fmt.Sprintf(`"%x"`, h.Sum(nil)[:12])
}

func (d *Driver) Get(ctx context.Context, key string) (*storage.Object, error) {
	defer trace.StartRegion(ctx, "fs.get").End()
	d.mu.RLock()
	defer d.mu.RUnlock()

	filePath := d.keyToPath(key)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("fs storage read error: %w", err)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("fs storage stat error: %w", err)
	}

	etag := d.generateETag(data, info.ModTime())

	return &storage.Object{
		Key:     key,
		Value:   data,
		ETag:    etag,
		ModTime: info.ModTime(),
	}, nil
}

func (d *Driver) Put(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	defer trace.StartRegion(ctx, "fs.put").End()
	d.mu.Lock()
	defer d.mu.Unlock()

	filePath := d.keyToPath(key)
	info, statErr := os.Stat(filePath)
	exists := statErr == nil

	if expectedETag != "" {
		if expectedETag == storage.MatchAnyETag {
			if exists {
				return nil, storage.ErrAlreadyExists
			}
		} else {
			if !exists {
				return nil, storage.ErrNotFound
			}
			existingData, err := os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("fs storage read existing error: %w", err)
			}
			currentETag := d.generateETag(existingData, info.ModTime())
			if currentETag != expectedETag {
				return nil, storage.ErrVersionMismatch
			}
		}
	}

	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, fmt.Errorf("fs storage mkdir error: %w", err)
	}

	tmpPath := fmt.Sprintf("%s.tmp.%d", filePath, time.Now().UnixNano())
	if err := os.WriteFile(tmpPath, value, 0644); err != nil {
		return nil, fmt.Errorf("fs storage write temp file error: %w", err)
	}

	if err := os.Rename(tmpPath, filePath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("fs storage atomic rename error: %w", err)
	}

	newInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("fs storage stat new file error: %w", err)
	}

	newETag := d.generateETag(value, newInfo.ModTime())

	return &storage.Object{
		Key:     key,
		Value:   append([]byte(nil), value...),
		ETag:    newETag,
		ModTime: newInfo.ModTime(),
	}, nil
}

func (d *Driver) Delete(ctx context.Context, key string, expectedETag string) error {
	defer trace.StartRegion(ctx, "fs.delete").End()
	d.mu.Lock()
	defer d.mu.Unlock()

	filePath := d.keyToPath(key)
	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return storage.ErrNotFound
		}
		return fmt.Errorf("fs storage stat delete error: %w", err)
	}

	if expectedETag != "" && expectedETag != storage.MatchAnyETag {
		data, readErr := os.ReadFile(filePath)
		if readErr != nil {
			return fmt.Errorf("fs storage read delete error: %w", readErr)
		}
		currentETag := d.generateETag(data, info.ModTime())
		if currentETag != expectedETag {
			return storage.ErrVersionMismatch
		}
	}

	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fs storage remove file error: %w", err)
	}

	return nil
}

func (d *Driver) List(ctx context.Context, prefix string, opts storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	defer trace.StartRegion(ctx, "fs.list").End()
	d.mu.RLock()
	defer d.mu.RUnlock()

	var results []*storage.KeyMeta

	err := filepath.Walk(d.rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".data") {
			return nil
		}

		rel, relErr := filepath.Rel(d.rootDir, path)
		if relErr != nil {
			return nil
		}

		key := strings.TrimSuffix(rel, ".data")
		key = filepath.ToSlash(key)

		if strings.HasPrefix(key, prefix) {
			if opts.Cursor != "" && key <= opts.Cursor {
				return nil
			}

			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil
			}
			etag := d.generateETag(data, info.ModTime())

			results = append(results, &storage.KeyMeta{
				Key:     key,
				Size:    info.Size(),
				ETag:    etag,
				ModTime: info.ModTime(),
			})
		}
		return nil
	})

	if err != nil {
		return nil, "", fmt.Errorf("fs storage walk error: %w", err)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	var nextCursor string
	if opts.Limit > 0 && len(results) > opts.Limit {
		nextCursor = results[opts.Limit-1].Key
		results = results[:opts.Limit]
	}

	return results, nextCursor, nil
}

func (d *Driver) Close() error {
	return nil
}
