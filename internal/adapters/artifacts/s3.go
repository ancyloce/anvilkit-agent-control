// Package artifacts holds the ArtifactStore adapter (DD-02 §6): the scoped
// object store of the eight artifact classes on an S3-compatible backend
// through the AWS SDK for Go v2, with its own bucket and credentials,
// separate from the obligation inventory's permission boundary. Objects
// are written by the uploader through a presigned, time-bounded PUT and
// read back by Control by their exact version; nothing here deletes.
package artifacts

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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

// S3Config names the artifact backend. The credentials are secrets and
// arrive only from the environment; they must be a different identity
// from the inventory's.
type S3Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	Prefix          string
	PathStyle       bool
	AccessKeyID     string
	SecretAccessKey string
}

// S3 is the artifact store on a versioned S3-compatible bucket.
type S3 struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	prefix  string
}

func NewS3(ctx context.Context, cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" || cfg.Endpoint == "" || cfg.Region == "" {
		return nil, fmt.Errorf("%w: artifacts s3 needs an endpoint, a region and a bucket", domain.ErrInvalid)
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("%w: artifacts s3 credentials are missing", domain.ErrInvalid)
	}
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
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
	return &S3{client: client, presign: s3.NewPresignClient(client), bucket: cfg.Bucket, prefix: prefix}, nil
}

// describe names an object in diagnostics by its class and transfer id
// (the last two segments of the key), never by bucket, prefix or URL.
func describe(key string) string {
	parts := strings.Split(strings.Trim(key, "/"), "/")
	if len(parts) >= 2 {
		return parts[len(parts)-2] + " artifact " + parts[len(parts)-1]
	}
	return "artifact " + key
}

func failure(op, key string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrInvalid) || errors.Is(err, domain.ErrUnavailable) {
		return err
	}
	return fmt.Errorf("%w: %s %s: %s", domain.ErrUnavailable, op, describe(key), classify(err))
}

func classify(err error) string {
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

func validKey(key string) error {
	if key == "" || strings.Contains(key, "..") || strings.HasPrefix(key, "/") {
		return fmt.Errorf("%w: artifact key", domain.ErrInvalid)
	}
	return nil
}

// emptyBodyMD5 is the Content-MD5 of an empty body, the S3 integrity
// header (RFC 1864 base64 of the MD5) that binds a zero-byte capability to
// its body: the SigV4 signer covers content-length only when it is
// positive, so a zero-length presigned PUT would otherwise accept a body
// of any size. The backend verifies Content-MD5 against the bytes it
// receives and refuses any other body (BadDigest). MD5 is the protocol's
// integrity field here, not a security digest; the transfer's sha256 is
// verified by Control at finalize.
var emptyBodyMD5 = base64.StdEncoding.EncodeToString(md5.New().Sum(nil))

// UploadCapability presigns one PUT of exactly size bytes of mediaType
// under the key, valid until expiresAt. The signed headers the backend
// will verify (the content type, the content length of a non-empty body,
// the Content-MD5 of an empty one) are returned with the URL so the
// uploader sends exactly them; a request with other values, or another
// body than declared, is refused by the backend. Qualify proves that
// refusal against the actual backend for both boundaries.
func (s *S3) UploadCapability(ctx context.Context, key, mediaType string, size int64, expiresAt time.Time) (application.UploadCapability, error) {
	if err := validKey(key); err != nil {
		return application.UploadCapability{}, err
	}
	if size < 0 {
		return application.UploadCapability{}, fmt.Errorf("%w: capability size must not be negative", domain.ErrInvalid)
	}
	ttl := time.Until(expiresAt)
	if ttl <= 0 {
		return application.UploadCapability{}, fmt.Errorf("%w: capability expiry already passed", domain.ErrInvalid)
	}
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + key), ContentType: aws.String(mediaType), ContentLength: aws.Int64(size),
	}
	if size == 0 {
		input.ContentMD5 = aws.String(emptyBodyMD5)
	}
	req, err := s.presign.PresignPutObject(ctx, input, s3.WithPresignExpires(ttl))
	if err != nil {
		return application.UploadCapability{}, failure("presign", key, err)
	}
	headers := map[string]string{}
	for name, values := range req.SignedHeader {
		if strings.EqualFold(name, "Host") || len(values) == 0 {
			continue
		}
		headers[http.CanonicalHeaderKey(name)] = values[0]
	}
	headers["Content-Type"] = mediaType
	headers["Content-Length"] = strconv.FormatInt(size, 10)
	return application.UploadCapability{URL: req.URL, Method: req.Method, Headers: headers, ExpiresAt: expiresAt}, nil
}

// Read fetches the exact object version and returns its bytes with the
// size and digest computed from them. A key or version that does not
// exist is domain.ErrNotFound; an object longer than limit is
// domain.ErrInvalid (the bytes are not returned); any other failure is
// domain.ErrUnavailable.
func (s *S3) Read(ctx context.Context, key, version string, limit int64) ([]byte, domain.ObjectFacts, error) {
	if err := validKey(key); err != nil {
		return nil, domain.ObjectFacts{}, err
	}
	if version == "" {
		return nil, domain.ObjectFacts{}, fmt.Errorf("%w: object version is required", domain.ErrInvalid)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + key), VersionId: aws.String(version)})
	if err != nil {
		var noKey *types.NoSuchKey
		var api smithy.APIError
		if errors.As(err, &noKey) || (errors.As(err, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound" || api.ErrorCode() == "NoSuchVersion" || api.ErrorCode() == "InvalidArgument")) {
			return nil, domain.ObjectFacts{}, fmt.Errorf("%w: %s version %q", domain.ErrNotFound, describe(key), version)
		}
		return nil, domain.ObjectFacts{}, failure("read", key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(io.LimitReader(out.Body, limit+1))
	if err != nil {
		return nil, domain.ObjectFacts{}, failure("read", key, err)
	}
	if int64(len(body)) > limit {
		return nil, domain.ObjectFacts{}, fmt.Errorf("%w: %s version %q holds more than the declared %d bytes", domain.ErrInvalid, describe(key), version, limit)
	}
	actual := aws.ToString(out.VersionId)
	if actual == "" || actual == "null" {
		return nil, domain.ObjectFacts{}, fmt.Errorf("%w: %s: the backend assigns no object versions (bucket versioning is off)", domain.ErrUnavailable, describe(key))
	}
	if actual != version {
		return nil, domain.ObjectFacts{}, fmt.Errorf("%w: %s: read version %q for the requested %q", domain.ErrUnavailable, describe(key), actual, version)
	}
	sum := sha256.Sum256(body)
	return body, domain.ObjectFacts{Version: actual, Size: int64(len(body)), Digest: domain.Digest("sha256:" + hex.EncodeToString(sum[:]))}, nil
}

// QualificationPrefix holds the probe objects of Qualify.
const QualificationPrefix = "qualification/"

// Qualify verifies, against the actual backend, what the transfer relies
// on: the bucket is versioned (every upload gets an immutable version id
// and an earlier version stays readable after a later upload of the key),
// a presigned PUT with the signed headers is accepted and one with other
// bytes than declared is refused, for a non-empty body (the signed length)
// and for an empty one (the signed Content-MD5, since no length is
// signed at zero), and the version read back is the one that was written.
// A backend that accepts a body other than declared on either boundary
// fails qualification and the store is not used. It proves nothing about
// the backend's placement, retention or failure domain.
func (s *S3) Qualify(ctx context.Context) error {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	key := QualificationPrefix + time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(nonce[:]) + "/probe"
	first, second := []byte(`{"class":"qualification","body":"first"}`), []byte(`{"class":"qualification","body":"second"}`)
	cap, err := s.UploadCapability(ctx, key, "application/json", int64(len(first)), time.Now().Add(2*time.Minute))
	if err != nil {
		return fmt.Errorf("presign: %w", err)
	}
	v1, err := Upload(ctx, cap, first)
	if err != nil {
		return fmt.Errorf("presigned upload: %w", err)
	}
	if v1 == "" {
		return errors.New("versioning: the backend returned no version id for the upload; enable bucket versioning")
	}
	if _, err := Upload(ctx, cap, append(first, '!')); err == nil {
		return errors.New("presigned upload: the backend accepted bytes other than the declared length")
	}
	if _, err := Upload(ctx, cap, first[:len(first)-1]); err == nil {
		return errors.New("presigned upload: the backend accepted fewer bytes than the declared length")
	}
	empty, err := s.UploadCapability(ctx, key+"-empty", "application/octet-stream", 0, time.Now().Add(2*time.Minute))
	if err != nil {
		return fmt.Errorf("presign (zero bytes): %w", err)
	}
	if _, err := Upload(ctx, empty, []byte("!")); err == nil {
		return errors.New("presigned upload: the backend accepted a non-empty body for a zero-byte capability (Content-MD5 not enforced); zero-byte transfers cannot be bound on this backend")
	}
	v0, err := Upload(ctx, empty, nil)
	if err != nil {
		return fmt.Errorf("presigned upload (zero bytes): %w", err)
	}
	if _, facts, err := s.Read(ctx, key+"-empty", v0, 0); err != nil || facts.Size != 0 {
		return fmt.Errorf("read by version (zero bytes): size %d, %v", facts.Size, err)
	}
	cap2, err := s.UploadCapability(ctx, key, "application/json", int64(len(second)), time.Now().Add(2*time.Minute))
	if err != nil {
		return fmt.Errorf("presign: %w", err)
	}
	v2, err := Upload(ctx, cap2, second)
	if err != nil {
		return fmt.Errorf("presigned upload: %w", err)
	}
	if v2 == v1 {
		return errors.New("versioning: a second upload of the key reused the version id")
	}
	body, facts, err := s.Read(ctx, key, v1, int64(len(second)))
	if err != nil {
		return fmt.Errorf("read by version: %w", err)
	}
	if !bytes.Equal(body, first) || facts.Version != v1 || facts.Size != int64(len(first)) {
		return errors.New("read by version: the earlier version did not read back unchanged after a later upload")
	}
	if _, _, err := s.Read(ctx, key, "no-such-version", 1024); !errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("read by version: an unknown version answered %v, not absence", err)
	}
	return nil
}

// Upload performs one presigned PUT as an uploader would (the
// qualification probe, tests) and returns the version id the backend
// assigned. It is the reference client of the capability: the headers as
// given, the body as declared.
func Upload(ctx context.Context, cap application.UploadCapability, body []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, cap.Method, cap.URL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	for k, v := range cap.Headers {
		if strings.EqualFold(k, "Content-Length") {
			continue // set from the body by the client, verified by the signature
		}
		req.Header.Set(k, v)
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.New("backend unreachable")
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("http %d", resp.StatusCode)
	}
	return resp.Header.Get("x-amz-version-id"), nil
}
