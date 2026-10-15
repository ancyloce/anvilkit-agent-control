package inventory_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// P3.7 AC3: the obligation inventory on a versioned, object-locked bucket (the
// DR store's anvilkit-inventory, deploy/qualification/bootstrap.py DR_BUCKETS).
// The adapter's conditional create, idempotent repeat and conflict refusal work
// there unchanged; a delete only hides the record behind a marker (the locked
// version stays readable), the locked version itself cannot be deleted within
// its retention, and an unconditional overwrite keeps the original version.
func TestS3InventoryOnAnObjectLockedBucket(t *testing.T) {
	b := startMinio(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const bucket = "anvilkit-inventory-locked"
	_, err := b.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket), ObjectLockEnabledForBucket: aws.Bool(true)})
	require.NoError(t, err)
	_, err = b.client.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{Bucket: aws.String(bucket),
		ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled,
			Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{Mode: types.ObjectLockRetentionModeGovernance, Days: aws.Int32(1)}}}})
	require.NoError(t, err)
	locked := *b
	locked.bucket = bucket
	inv := locked.adapter(t, "p37/")

	body := []byte(`{"class":"intake","operationId":"op_locked"}`)
	v1, err := inv.Put(ctx, "intake/op_locked.json", body)
	require.NoError(t, err, "a conditional create on a locked bucket")
	v2, err := inv.Put(ctx, "intake/op_locked.json", body)
	require.NoError(t, err, "the same body again is idempotent")
	require.Equal(t, v1, v2)
	_, err = inv.Put(ctx, "intake/op_locked.json", []byte(`{"class":"intake","operationId":"other"}`))
	require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "a different body never replaces the record")

	versions, err := b.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: aws.String("p37/intake/op_locked.json")})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 1)
	original := versions.Versions[0].VersionId

	// An unconditional overwrite (a writer that bypasses the adapter) and a
	// delete: the original version is still there to read.
	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("p37/intake/op_locked.json"),
		Body: bytes.NewReader([]byte(`{"tampered":true}`)), ChecksumAlgorithm: types.ChecksumAlgorithmCrc32})
	require.NoError(t, err)
	_, err = b.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("p37/intake/op_locked.json")})
	require.NoError(t, err, "an unversioned delete only adds a marker")
	got, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("p37/intake/op_locked.json"), VersionId: original})
	require.NoError(t, err)
	raw, err := io.ReadAll(got.Body)
	require.NoError(t, err)
	require.Equal(t, body, raw, "the locked original survives an overwrite and a delete")
	// Deleting the locked version itself is refused within its retention.
	_, err = b.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("p37/intake/op_locked.json"), VersionId: original})
	require.Error(t, err, "a locked version cannot be deleted before its retention ends")
}
