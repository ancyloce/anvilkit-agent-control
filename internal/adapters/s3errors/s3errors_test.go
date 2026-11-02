package s3errors

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// responseError is the SDK's shape of an answered (status > 0) or
// unanswered (status 0) request wrapping err.
func responseError(status int, err error) error {
	return &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      err,
	}}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestClassifyNamesTheErrorWithoutItsMessage(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused https://bucket.example/full/key")}
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"deadline", fmt.Errorf("get: %w", context.DeadlineExceeded), "timeout"},
		{"canceled", fmt.Errorf("get: %w", context.Canceled), "canceled"},
		{"api error with status", responseError(503, &smithy.GenericAPIError{Code: "SlowDown", Message: "https://bucket.example/full/key"}), "api error SlowDown (http 503)"},
		{"api error without response", &smithy.GenericAPIError{Code: "AccessDenied"}, "api error AccessDenied"},
		{"answered without api error", responseError(500, errors.New("https://bucket.example/full/key")), "http 500"},
		{"unanswered transport failure", responseError(0, refused), "backend unreachable"},
		{"network timeout", fmt.Errorf("get: %w", timeoutError{}), "timeout"},
		{"anything else", errors.New("https://bucket.example/full/key"), "backend error"},
	}
	for _, c := range cases {
		if got := Classify(c.err); got != c.want {
			t.Errorf("%s: Classify = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestMissingIsTheTypedNoSuchKeyOrANamedCode(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		codes []string
		want  bool
	}{
		{"typed NoSuchKey, no codes named", fmt.Errorf("get: %w", &types.NoSuchKey{}), nil, true},
		{"named code", responseError(404, &smithy.GenericAPIError{Code: "NoSuchVersion"}), []string{"NoSuchKey", "NoSuchVersion"}, true},
		{"code not named by this adapter", responseError(400, &smithy.GenericAPIError{Code: "InvalidArgument"}), []string{"NoSuchKey", "NotFound"}, false},
		{"other api error", &smithy.GenericAPIError{Code: "AccessDenied"}, []string{"NoSuchKey", "NotFound"}, false},
		{"not an api error", errors.New("NoSuchKey"), []string{"NoSuchKey"}, false},
	}
	for _, c := range cases {
		if got := Missing(c.err, c.codes...); got != c.want {
			t.Errorf("%s: Missing = %v, want %v", c.name, got, c.want)
		}
	}
}
