package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity"
)

func verified(w identity.Workload) context.Context {
	p := identity.Principal{TrustDomain: "anvilkit.local", Namespace: w.Namespace, ServiceAccount: w.ServiceAccount}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: identity.PeerInfo{Principal: p}})
}

// P0.2 AC1/AC2: a field naming the calling workload is derived from the
// verified peer; a request naming anyone else is PERMISSION_DENIED; the
// development listener (no verified peer) keeps the request's statement.
func TestAuthorityFieldsFollowTheVerifiedCaller(t *testing.T) {
	proxyCtx := verified(modelProxy)
	owner, err := callingOwner(proxyCtx, "")
	require.NoError(t, err)
	require.Equal(t, "anvilkit-agent-model-proxy", owner, "an empty owner is the caller")
	owner, err = callingOwner(proxyCtx, "spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-model-proxy")
	require.NoError(t, err)
	require.Equal(t, "anvilkit-agent-model-proxy", owner, "the complete ID names the caller")
	_, err = callingOwner(proxyCtx, "anvilkit-agent-mcp")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the proxy may not admit under the MCP owner")
	_, err = callingOwner(verified(workflow), "anvilkit-agent-model-proxy")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the Workflow may not claim the proxy's owner")
	owner, err = callingOwner(context.Background(), "proxy-a")
	require.NoError(t, err)
	require.Equal(t, "proxy-a", owner, "DEVELOPMENT_ONLY: the plaintext listener verifies nobody")

	observer, err := callingObserver(verified(sidecar), "anvilkit-job-access-sidecar")
	require.NoError(t, err)
	require.Equal(t, "spiffe://anvilkit.local/ns/anvilkit-components/sa/anvilkit-job-access-sidecar", observer, "the recorded observer is the SAN")
	_, err = callingObserver(verified(sidecar), "anvilkit-codegen-supervisor")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "an in-Pod label is not the verified observer")
	_, err = callingObserver(verified(identity.Workload{Namespace: "anvilkit-apps", ServiceAccount: "anvilkit-job-access-sidecar"}), "anvilkit-job-access-sidecar")
	require.NoError(t, err, "the namespace is part of the identity; the policy, not this helper, refuses it")

	reader, err := dispatchReader(proxyCtx, "tenant_a", "")
	require.NoError(t, err)
	require.Equal(t, application.Reader{TenantID: "tenant_a", Owner: "anvilkit-agent-model-proxy"}, reader, "an owner reads its own dispatches")
	_, err = dispatchReader(verified(mcp), "tenant_a", "anvilkit-agent-model-proxy")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "MCP may not read as the proxy")
	reader, err = dispatchReader(verified(knowledge), "tenant_a", "anvilkit-agent-knowledge")
	require.NoError(t, err)
	require.Equal(t, application.Reader{TenantID: "tenant_a"}, reader, "a non-owner reader is bound to its tenant")
}
