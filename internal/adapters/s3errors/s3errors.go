// Package s3errors classifies the AWS SDK for Go v2 errors of Control's two
// S3-compatible adapters, the artifact store and the obligation inventory.
// It holds no client, bucket or credential: each adapter keeps its own
// identity, bucket, key naming and error sentinels, so their permission
// boundaries stay apart; only the reading of an SDK error is shared.
package s3errors

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Classify names an SDK error for a controlled diagnostic: the API error
// code and HTTP status when the backend answered, a transport
// classification when it did not. The SDK's own message (which can carry
// the request URL and therefore the bucket and full key) never propagates.
func Classify(err error) string {
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
		if errors.As(err, &resp) && resp.HTTPStatusCode() > 0 {
			code += " (http " + strconv.Itoa(resp.HTTPStatusCode()) + ")"
		}
		return code
	// A response error without a status is a transport failure the SDK
	// wrapped before any answer existed (a refused connection on some
	// hosts): it is unreachability, not an HTTP outcome.
	case errors.As(err, &resp) && resp.HTTPStatusCode() > 0:
		return "http " + strconv.Itoa(resp.HTTPStatusCode())
	case errors.As(err, &netErr):
		if netErr.Timeout() {
			return "timeout"
		}
		return "backend unreachable"
	}
	return "backend error"
}

// Missing reports the backend's answer that the object does not exist: the
// SDK's typed NoSuchKey, or an API error whose code is one of codes. Each
// adapter names the codes that mean absence for its own reads.
func Missing(err error, codes ...string) bool {
	var noKey *types.NoSuchKey
	if errors.As(err, &noKey) {
		return true
	}
	var api smithy.APIError
	return errors.As(err, &api) && slices.Contains(codes, api.ErrorCode())
}
