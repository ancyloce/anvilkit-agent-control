// Package s3client builds the S3 client of Control's two S3-compatible
// adapters, the obligation inventory and the artifact store. Each adapter
// calls New with its own placement and credentials and keeps the client it
// gets: nothing is shared at run time, so their identities, buckets and
// permission boundaries stay apart; only the construction is shared.
package s3client

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Config names one backend, its bucket and key prefix. The credentials are
// secrets and arrive only from the environment or the mounted secret file
// their *_file key names (config.InventoryS3). Endpoint is the S3 API of
// the backend (an RGW endpoint, or an S3-compatible development store);
// PathStyle addresses the bucket in the path, which RGW and MinIO
// deployments commonly need.
type Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	Prefix          string
	PathStyle       bool
	AccessKeyID     string
	SecretAccessKey string
}

// RequestTimeout bounds one request to the backend (P1.4): a hung store
// never holds a Control transaction's caller open.
const RequestTimeout = 60 * time.Second

// New validates cfg, naming the adapter as what in its refusals, and
// returns the client with the key prefix normalized to "" or "<prefix>/".
// The SDK's own retry loop is off: it would repeat a request whose first
// answer was lost (an inventory conditional create, an artifact read), and
// each adapter reenters under its own rules instead. Request checksum
// calculation and response validation are WhenRequired: the default
// WhenSupported adds CRC-based trailers that S3-compatible backends do not
// uniformly accept.
func New(ctx context.Context, what string, cfg Config) (*s3.Client, string, error) {
	if cfg.Bucket == "" || cfg.Endpoint == "" || cfg.Region == "" {
		return nil, "", fmt.Errorf("%w: %s needs an endpoint, a region and a bucket", domain.ErrInvalid, what)
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, "", fmt.Errorf("%w: %s credentials are missing", domain.ErrInvalid, what)
	}
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		config.WithRetryMaxAttempts(1),
	)
	if err != nil {
		return nil, "", err
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = cfg.PathStyle
		o.HTTPClient = &http.Client{Timeout: RequestTimeout}
	})
	prefix := strings.Trim(cfg.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return client, prefix, nil
}
