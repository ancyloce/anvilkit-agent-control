package inventory

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// S3Config names the backend, the bucket and the key prefix of the
// obligation inventory. The credentials are secrets and arrive only from
// the environment. Endpoint is the S3 API of the backend (an RGW
// endpoint, or an S3-compatible development store); PathStyle addresses
// the bucket in the path, which RGW and MinIO deployments commonly need.
type S3Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	Prefix          string
	PathStyle       bool
	AccessKeyID     string
	SecretAccessKey string
}

// S3 is the obligation inventory on an S3-compatible backend through the
// AWS SDK for Go v2. Conditional creation uses the If-None-Match: *
// precondition of PutObject; a 412 answer is the backend's statement that
// the key exists, after which the existing body is read and compared.
// Versions are the backend's VersionId when the bucket is versioned,
// otherwise the ETag. Listing uses ListObjectsV2 continuation tokens so an
// interrupted enumeration resumes from its cursor.
//
// The SDK's request checksum calculation and response validation are set
// to WhenRequired: the default WhenSupported adds CRC-based trailers that
// S3-compatible backends do not uniformly accept.
type S3 struct {
	client *s3.Client
	bucket string
	prefix string
}

func NewS3(ctx context.Context, cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" || cfg.Endpoint == "" || cfg.Region == "" {
		return nil, fmt.Errorf("%w: inventory s3 needs an endpoint, a region and a bucket", domain.ErrInvalid)
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("%w: inventory s3 credentials are missing", domain.ErrInvalid)
	}
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		// The SDK's own retry loop would repeat a conditional create whose
		// first answer was lost; the application reenters the same key and
		// body under its own rules instead.
		config.WithRetryMaxAttempts(1),
	)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = cfg.PathStyle
	})
	prefix := strings.Trim(cfg.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return &S3{client: client, bucket: cfg.Bucket, prefix: prefix}, nil
}

func validKey(key string) error {
	if key == "" || strings.Contains(key, "..") || strings.HasPrefix(key, "/") || strings.Contains(key, tempSuffix) {
		return fmt.Errorf("%w: inventory key %s", domain.ErrInvalid, application.DescribeKey(key))
	}
	return nil
}

// s3Failure classifies an SDK error into a controlled diagnostic: the
// operation, the object's controlled identity, the API error code or HTTP
// status when the backend answered, and a transport classification when
// it did not. The SDK's own message (which can carry the request URL and
// therefore the bucket and full key) never propagates.
func s3Failure(op, key string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrIdempotencyConflict) || errors.Is(err, domain.ErrInvalid) || errors.Is(err, ErrUnavailable) {
		return err
	}
	return fmt.Errorf("%w: %s inventory %s: %s", ErrUnavailable, op, application.DescribeKey(key), classifyS3(err))
}

func classifyS3(err error) string {
	var api smithy.APIError
	var resp *awshttp.ResponseError
	var netErr net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.As(err, &api):
		code := "api error " + api.ErrorCode()
		if errors.As(err, &resp) {
			code += " (http " + strconv.Itoa(resp.HTTPStatusCode()) + ")"
		}
		return code
	case errors.As(err, &resp):
		return "http " + strconv.Itoa(resp.HTTPStatusCode())
	case errors.As(err, &netErr):
		if netErr.Timeout() {
			return "timeout"
		}
		return "backend unreachable"
	}
	return "backend error"
}

func version(versionID, etag *string) string {
	if versionID != nil && *versionID != "" && *versionID != "null" {
		return "version:" + *versionID
	}
	if etag != nil {
		return "etag:" + strings.Trim(*etag, "\"")
	}
	return ""
}

// preconditionFailed reports the backend's answer that the key already
// exists: 412 PreconditionFailed, or 409 ConditionalRequestConflict when
// two conditional creates of the key raced.
func preconditionFailed(err error) bool {
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
	}
	return false
}

// Put creates the object conditionally. A 412 means the key exists: the
// existing body is read and compared, so the same body is idempotent and a
// different body is domain.ErrIdempotencyConflict. Any other failure, the
// read after a 412 included, leaves the write uncertain and is reported as
// ErrUnavailable with a controlled classification.
func (s *S3) Put(ctx context.Context, key string, body []byte) (string, error) {
	if err := validKey(key); err != nil {
		return "", err
	}
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + key), Body: bytes.NewReader(body),
		ContentType: aws.String("application/json"), IfNoneMatch: aws.String("*"),
	})
	if err == nil {
		return version(out.VersionId, out.ETag), nil
	}
	if !preconditionFailed(err) {
		return "", s3Failure("put", key, err)
	}
	existing, ver, gerr := s.Get(ctx, key)
	if gerr != nil {
		return "", fmt.Errorf("%w: exists but could not be read back", s3Failure("put", key, gerr))
	}
	if err := compare(key, existing, body); err != nil {
		return "", err
	}
	return ver, nil
}

// Get reads the object; a missing key is domain.ErrNotFound, any other
// failure ErrUnavailable.
func (s *S3) Get(ctx context.Context, key string) ([]byte, string, error) {
	if err := validKey(key); err != nil {
		return nil, "", err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + key)})
	if err != nil {
		var noKey *types.NoSuchKey
		var api smithy.APIError
		if errors.As(err, &noKey) || (errors.As(err, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound")) {
			return nil, "", fmt.Errorf("%w: inventory %s", domain.ErrNotFound, application.DescribeKey(key))
		}
		return nil, "", s3Failure("get", key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", s3Failure("get", key, err)
	}
	return body, version(out.VersionId, out.ETag), nil
}

// List enumerates one page under prefix; cursor is the continuation token
// of the previous page. A failed request is ErrUnavailable, never an
// empty page; a prefix with no objects under an answering backend is an
// empty, complete listing.
func (s *S3) List(ctx context.Context, prefix, cursor string, limit int) (application.InventoryPage, error) {
	if strings.Contains(prefix, "..") || strings.HasPrefix(prefix, "/") {
		return application.InventoryPage{}, fmt.Errorf("%w: inventory prefix %q", domain.ErrInvalid, prefix)
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	in := &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(s.prefix + prefix), MaxKeys: aws.Int32(int32(limit))}
	if cursor != "" {
		in.ContinuationToken = aws.String(cursor)
	}
	out, err := s.client.ListObjectsV2(ctx, in)
	if err != nil {
		return application.InventoryPage{}, s3Failure("list", prefix, err)
	}
	page := application.InventoryPage{Complete: !aws.ToBool(out.IsTruncated)}
	for _, o := range out.Contents {
		key := strings.TrimPrefix(aws.ToString(o.Key), s.prefix)
		if strings.Contains(key, tempSuffix) {
			continue
		}
		obj := application.InventoryObject{Key: key, Version: version(nil, o.ETag), Size: aws.ToInt64(o.Size)}
		if o.LastModified != nil {
			obj.ModifiedAt = o.LastModified.UTC()
		}
		page.Objects = append(page.Objects, obj)
	}
	if !page.Complete {
		page.NextCursor = aws.ToString(out.NextContinuationToken)
		if page.NextCursor == "" {
			return application.InventoryPage{}, fmt.Errorf("%w: list inventory %s: truncated listing without a continuation token", ErrUnavailable, application.DescribeKey(prefix))
		}
	}
	return page, nil
}

// QualificationPrefix holds the probe objects of Qualify.
const QualificationPrefix = "qualification/"

// Qualify verifies, against the actual backend, the behavior the
// application relies on: a conditional create is refused for an existing
// key (the backend honors If-None-Match, so a different body never
// overwrites an obligation), the same body is idempotent with the same
// version, the object reads back at once with the body written, and a
// directory-style prefix listing enumerates every object under it across
// pages with a resumable cursor. It proves nothing about failure-domain
// independence, retention or replication (ENV-02); a backend that fails
// any check must not hold the inventory.
//
// Control lists only "<class>/" prefixes. A prefix that names an object
// exactly is answered by at least one S3-compatible backend (MinIO
// RELEASE.2025-09-07) with that single object and no truncation even when
// longer keys share the prefix; the probe therefore lists a directory
// prefix, which is the only form the application uses.
func (s *S3) Qualify(ctx context.Context) error {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	dir := QualificationPrefix + time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(nonce[:]) + "/"
	probe, sibling := dir+"a", dir+"b"
	first := []byte(`{"class":"qualification","probe":"` + probe + `","body":"first"}`)
	second := []byte(`{"class":"qualification","probe":"` + probe + `","body":"second"}`)
	v1, err := s.Put(ctx, probe, first)
	if err != nil {
		return fmt.Errorf("conditional create: %w", err)
	}
	v2, err := s.Put(ctx, probe, first)
	if err != nil {
		return fmt.Errorf("same-body idempotency: %w", err)
	}
	if v1 != v2 {
		return fmt.Errorf("same-body idempotency: versions differ (%s, %s); the backend created a second version", v1, v2)
	}
	if _, err := s.Put(ctx, probe, second); !errors.Is(err, domain.ErrIdempotencyConflict) {
		return fmt.Errorf("conditional create is not enforced: a different body for an existing key returned %v", err)
	}
	body, v3, err := s.Get(ctx, probe)
	if err != nil {
		return fmt.Errorf("read after write: %w", err)
	}
	if !bytes.Equal(body, first) || v3 != v1 {
		return fmt.Errorf("read after write: the object was overwritten or its version changed (%s, %s)", v1, v3)
	}
	if sum := sha256.Sum256(body); sum != sha256.Sum256(first) {
		return errors.New("read after write: body digest differs")
	}
	// Pagination: two probes under the directory prefix must come back
	// across two pages with a cursor between them and nothing missing.
	if _, err := s.Put(ctx, sibling, first); err != nil {
		return fmt.Errorf("pagination probe: %w", err)
	}
	p1, err := s.List(ctx, dir, "", 1)
	if err != nil || p1.Complete || len(p1.Objects) != 1 || p1.NextCursor == "" || p1.Objects[0].Key != probe {
		return fmt.Errorf("pagination: first page complete=%v objects=%d cursor=%q err=%v", p1.Complete, len(p1.Objects), p1.NextCursor, err)
	}
	p2, err := s.List(ctx, dir, p1.NextCursor, 1)
	if err != nil || !p2.Complete || len(p2.Objects) != 1 || p2.Objects[0].Key != sibling {
		return fmt.Errorf("pagination: second page complete=%v objects=%d err=%v", p2.Complete, len(p2.Objects), err)
	}
	return nil
}
