package inventory_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// minioImage is the S3-compatible development store these tests run
// against (DEVELOPMENT_ONLY). It exercises the adapter's use of the SDK
// and the conditional-create and listing contract on a real S3 API; it is
// not the Ceph RGW backend of C09, and passing here qualifies neither RGW
// nor the independent failure domain of ENV-02.
const minioImage = "minio/minio:RELEASE.2025-09-07T16-13-09Z"

type s3Backend struct {
	endpoint string
	bucket   string
	client   *s3.Client
}

func startMinio(t *testing.T) *s3Backend {
	t.Helper()
	if os.Getenv("ANVILKIT_SKIP_DOCKER_TESTS") != "" {
		t.Skip("ANVILKIT_SKIP_DOCKER_TESTS set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: minioImage, Cmd: []string{"server", "/data"}, ExposedPorts: []string{"9000/tcp"},
			Env:        map[string]string{"MINIO_ROOT_USER": "minioadmin", "MINIO_ROOT_PASSWORD": "minioadmin"},
			WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start minio: %v", err)
	}
	testcontainers.CleanupContainer(t, c)
	endpoint, err := c.PortEndpoint(ctx, "9000/tcp", "http")
	require.NoError(t, err)
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired))
	require.NoError(t, err)
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("anvilkit-inventory")})
	require.NoError(t, err)
	return &s3Backend{endpoint: endpoint, bucket: "anvilkit-inventory", client: client}
}

func (b *s3Backend) adapter(t *testing.T, prefix string) *inventory.S3 {
	t.Helper()
	inv, err := inventory.NewS3(context.Background(), inventory.S3Config{
		Endpoint: b.endpoint, Region: "us-east-1", Bucket: b.bucket, Prefix: prefix, PathStyle: true,
		AccessKeyID: "minioadmin", SecretAccessKey: "minioadmin",
	})
	require.NoError(t, err)
	return inv
}

func TestS3Inventory(t *testing.T) {
	backend := startMinio(t)
	ctx := context.Background()
	inv := backend.adapter(t, "control")

	t.Run("the backend passes the qualification probe", func(t *testing.T) {
		require.NoError(t, inv.Qualify(ctx))
	})

	t.Run("conditional create is idempotent for the same body and conflicts for a different one", func(t *testing.T) {
		body := []byte(`{"class":"intake","operationId":"op_1"}`)
		v1, err := inv.Put(ctx, "intake/op_1", body)
		require.NoError(t, err)
		require.NotEmpty(t, v1)
		v2, err := inv.Put(ctx, "intake/op_1", body)
		require.NoError(t, err, "a retry after a lost acknowledgement succeeds")
		require.Equal(t, v1, v2, "same body, same immutable version")
		_, err = inv.Put(ctx, "intake/op_1", []byte(`{"class":"intake","operationId":"op_1","extra":true}`))
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict)
		got, v3, err := inv.Get(ctx, "intake/op_1")
		require.NoError(t, err)
		require.Equal(t, body, got, "the published object is never overwritten")
		require.Equal(t, v1, v3)
		_, _, err = inv.Get(ctx, "intake/op_missing")
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("a raw unconditional overwrite is what the adapter never issues", func(t *testing.T) {
		// The probe proves the backend refuses If-None-Match on an existing
		// key; the adapter always sends it, so this is the only way a body
		// could change, and it is outside the adapter.
		_, err := backend.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(backend.bucket), Key: aws.String("control/intake/op_1"), Body: nil, IfNoneMatch: aws.String("*")})
		require.Error(t, err, "the backend enforces the precondition")
	})

	t.Run("listing is ordered, paginated by continuation token and scoped to the prefix", func(t *testing.T) {
		for _, id := range []string{"c", "a", "b"} {
			_, err := inv.Put(ctx, "model-dispatch/dsp_"+id, []byte(`{"class":"model-dispatch","dispatchId":"dsp_`+id+`"}`))
			require.NoError(t, err)
		}
		other := backend.adapter(t, "other")
		_, err := other.Put(ctx, "model-dispatch/dsp_z", []byte(`{}`))
		require.NoError(t, err)
		p1, err := inv.List(ctx, "model-dispatch/", "", 2)
		require.NoError(t, err)
		require.False(t, p1.Complete)
		require.NotEmpty(t, p1.NextCursor)
		require.Equal(t, []string{"model-dispatch/dsp_a", "model-dispatch/dsp_b"}, keys(p1))
		p2, err := inv.List(ctx, "model-dispatch/", p1.NextCursor, 2)
		require.NoError(t, err)
		require.True(t, p2.Complete)
		require.Equal(t, []string{"model-dispatch/dsp_c"}, keys(p2), "another prefix's objects are never listed")
		for _, o := range append(p1.Objects, p2.Objects...) {
			require.NotEmpty(t, o.Version)
			require.False(t, o.ModifiedAt.IsZero())
		}
	})

	t.Run("an unreachable backend is an error, never an empty listing", func(t *testing.T) {
		broken, err := inventory.NewS3(ctx, inventory.S3Config{Endpoint: "http://127.0.0.1:1", Region: "us-east-1", Bucket: backend.bucket, PathStyle: true, AccessKeyID: "x", SecretAccessKey: "y"})
		require.NoError(t, err)
		short, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err = broken.List(short, "intake/", "", 10)
		require.ErrorIs(t, err, inventory.ErrUnavailable)
		_, err = broken.Put(short, "intake/op_x", []byte(`{}`))
		require.ErrorIs(t, err, inventory.ErrUnavailable, "an uncertain write is reported, not swallowed")
		_, _, err = broken.Get(short, "intake/op_1")
		require.ErrorIs(t, err, inventory.ErrUnavailable)
		require.NotErrorIs(t, err, domain.ErrNotFound, "unavailability is not absence")
		// Diagnostics carry the controlled identity and a classification,
		// never the endpoint, the bucket or the full object key.
		for _, forbidden := range []string{"127.0.0.1:1", backend.bucket, "control/intake", "http"} {
			require.NotContains(t, err.Error(), forbidden, "%v", err)
		}
		require.Contains(t, err.Error(), "intake object op_1")
	})
}
