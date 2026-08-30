package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/trace"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/avivklas/jaydb/pkg/storage"
)

// Config defines connection details for an S3 / S3-compatible bucket endpoint.
type Config struct {
	Endpoint     string // e.g. "https://s3.amazonaws.com" or "http://localhost:9000"
	Bucket       string
	Region       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Prefix       string
	HTTPClient   *http.Client
}

// Driver implements storage.Driver using official AWS SDK v2 for S3 with SigV4 and IAM role support.
type Driver struct {
	cfg    Config
	client *s3.Client
	bucket string
	prefix string
}

// NewDriver initializes a new AWS SDK v2 S3 storage driver.
func NewDriver(cfg Config) (storage.Driver, error) {
	ctx := context.Background()
	region := cfg.Region
	if region == "" {
		region = os.Getenv("JAYDB_S3_REGION")
	}
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		region = "us-east-1"
	}

	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(region))

	if cfg.HTTPClient != nil {
		opts = append(opts, config.WithHTTPClient(cfg.HTTPClient))
	}

	sessionToken := cfg.SessionToken
	if sessionToken == "" {
		sessionToken = os.Getenv("AWS_SESSION_TOKEN")
	}

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, sessionToken)))
	} else if cfg.Endpoint != "" || cfg.HTTPClient != nil {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy-key", "dummy-secret", "")))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	})

	prefix := strings.Trim(cfg.Prefix, "/")

	return &Driver{
		cfg:    cfg,
		client: s3Client,
		bucket: cfg.Bucket,
		prefix: prefix,
	}, nil
}

func (d *Driver) resolveKey(key string) string {
	cleanKey := strings.TrimPrefix(key, "/")
	if d.prefix == "" {
		return cleanKey
	}
	return d.prefix + "/" + cleanKey
}

func (d *Driver) Get(ctx context.Context, key string) (*storage.Object, error) {
	defer trace.StartRegion(ctx, "s3.get").End()
	s3Key := d.resolveKey(key)
	out, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		var noKey *types.NoSuchKey
		var notFound *types.NotFound
		if errors.As(err, &noKey) || errors.As(err, &notFound) {
			return nil, storage.ErrNotFound
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "404") {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("s3 get failed: %w", err)
	}
	defer out.Body.Close()

	bodyRegion := trace.StartRegion(ctx, "s3.read_body")
	data, err := io.ReadAll(out.Body)
	bodyRegion.End()
	if err != nil {
		return nil, fmt.Errorf("s3 read body failed: %w", err)
	}

	etag := ""
	if out.ETag != nil {
		etag = strings.Trim(*out.ETag, `"`)
		etag = fmt.Sprintf(`"%s"`, etag)
	}

	modTime := time.Now()
	if out.LastModified != nil {
		modTime = *out.LastModified
	}

	return &storage.Object{
		Key:     key,
		Value:   data,
		ETag:    etag,
		ModTime: modTime,
	}, nil
}

func (d *Driver) Put(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	defer trace.StartRegion(ctx, "s3.put").End()
	s3Key := d.resolveKey(key)
	input := &s3.PutObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(s3Key),
		Body:   bytes.NewReader(value),
	}

	if expectedETag == storage.MatchAnyETag {
		input.IfNoneMatch = aws.String("*")
	} else if expectedETag != "" {
		cleanETag := strings.Trim(expectedETag, `"`)
		input.IfMatch = aws.String(fmt.Sprintf(`"%s"`, cleanETag))
	}

	out, err := d.client.PutObject(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "PreconditionFailed" || apiErr.ErrorCode() == "412" || apiErr.ErrorCode() == "AccessDenied") {
			if expectedETag == storage.MatchAnyETag {
				return nil, storage.ErrAlreadyExists
			}
			return nil, storage.ErrVersionMismatch
		}
		return nil, fmt.Errorf("s3 put failed: %w", err)
	}

	etag := ""
	if out.ETag != nil {
		etag = strings.Trim(*out.ETag, `"`)
		etag = fmt.Sprintf(`"%s"`, etag)
	}

	return &storage.Object{
		Key:     key,
		Value:   value,
		ETag:    etag,
		ModTime: time.Now(),
	}, nil
}

func (d *Driver) Delete(ctx context.Context, key string, expectedETag string) error {
	defer trace.StartRegion(ctx, "s3.delete").End()
	s3Key := d.resolveKey(key)
	_, err := d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return fmt.Errorf("s3 delete failed: %w", err)
	}
	return nil
}

func (d *Driver) List(ctx context.Context, prefix string, opts storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	defer trace.StartRegion(ctx, "s3.list").End()
	s3Prefix := d.resolveKey(prefix)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucket),
		Prefix: aws.String(s3Prefix),
	}
	if opts.Limit > 0 {
		input.MaxKeys = aws.Int32(int32(opts.Limit))
	}
	if opts.Cursor != "" {
		input.ContinuationToken = aws.String(opts.Cursor)
	}

	out, err := d.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, "", fmt.Errorf("s3 list failed: %w", err)
	}

	var results []*storage.KeyMeta
	for _, obj := range out.Contents {
		if obj.Key == nil {
			continue
		}
		k := *obj.Key
		if d.prefix != "" {
			k = strings.TrimPrefix(k, d.prefix+"/")
		}
		etag := ""
		if obj.ETag != nil {
			etag = strings.Trim(*obj.ETag, `"`)
			etag = fmt.Sprintf(`"%s"`, etag)
		}
		modTime := time.Now()
		if obj.LastModified != nil {
			modTime = *obj.LastModified
		}

		var size int64
		if obj.Size != nil {
			size = *obj.Size
		}

		results = append(results, &storage.KeyMeta{
			Key:     k,
			Size:    size,
			ETag:    etag,
			ModTime: modTime,
		})
	}

	nextCursor := ""
	if out.NextContinuationToken != nil {
		nextCursor = *out.NextContinuationToken
	}

	return results, nextCursor, nil
}

func (d *Driver) Close() error {
	return nil
}
