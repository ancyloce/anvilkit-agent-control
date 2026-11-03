package s3client_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/s3client"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Each adapter's placement is refused by its own name before any client
// exists, and the key prefix is normalized to "" or "<prefix>/".
func TestNewValidatesThePlacementAndNormalizesThePrefix(t *testing.T) {
	ctx := context.Background()
	full := s3client.Config{Endpoint: "https://s3.example", Region: "us-east-1", Bucket: "b", AccessKeyID: "id", SecretAccessKey: "key"}

	noBucket := full
	noBucket.Bucket = ""
	_, _, err := s3client.New(ctx, "inventory s3", noBucket)
	require.ErrorIs(t, err, domain.ErrInvalid)
	require.ErrorContains(t, err, "inventory s3 needs an endpoint, a region and a bucket")

	noSecret := full
	noSecret.SecretAccessKey = ""
	_, _, err = s3client.New(ctx, "artifacts s3", noSecret)
	require.ErrorIs(t, err, domain.ErrInvalid)
	require.ErrorContains(t, err, "artifacts s3 credentials are missing")

	for in, want := range map[string]string{"": "", "/": "", "artifacts": "artifacts/", "/inventory/": "inventory/"} {
		withPrefix := full
		withPrefix.Prefix = in
		client, prefix, err := s3client.New(ctx, "inventory s3", withPrefix)
		require.NoError(t, err)
		require.NotNil(t, client)
		require.Equal(t, want, prefix, "prefix %q", in)
	}
}
