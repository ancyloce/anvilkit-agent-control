package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/transport/identity"
)

// Authority fields derived from the caller (P0.2). On the mTLS listener
// every call carries its peer's verified workload identity (the Authorizer
// refused it otherwise). A request field that names the calling workload —
// the owner of a dispatch, the observer of a result — must name that
// identity, by ServiceAccount or by complete SPIFFE ID, and the identity is
// what Control records; a different value is PERMISSION_DENIED. The
// DEVELOPMENT_ONLY plaintext listener verifies no workload, and there the
// request's statement is taken as made.

func namesCaller(stated string, p identity.Principal) bool {
	return stated == "" || stated == p.ServiceAccount || stated == p.String()
}

func misnamed(stated string, p identity.Principal) error {
	return status.Errorf(codes.PermissionDenied, "PERMISSION_DENIED: the request names %q, the caller is %s", stated, p)
}

// callingOwner is the owner identity a dispatch owner acts as: its
// ServiceAccount.
func callingOwner(ctx context.Context, stated string) (string, error) {
	p, ok := identity.Caller(ctx)
	if !ok {
		return stated, nil
	}
	if !namesCaller(stated, p) {
		return "", misnamed(stated, p)
	}
	return p.ServiceAccount, nil
}

// callingObserver is the observer identity Control records for a result:
// the caller's complete SPIFFE ID.
func callingObserver(ctx context.Context, stated string) (string, error) {
	p, ok := identity.Caller(ctx)
	if !ok {
		return stated, nil
	}
	if !namesCaller(stated, p) {
		return "", misnamed(stated, p)
	}
	return p.String(), nil
}

func isWorkload(p identity.Principal, w identity.Workload) bool {
	return p.Namespace == w.Namespace && p.ServiceAccount == w.ServiceAccount
}

// dispatchReader binds a dispatch read or report to the tenant it acts for
// and, when the verified caller is a dispatch owner (the Model Proxy, MCP),
// to that owner: an owner reads and reports only its own dispatches.
// Other verified readers (Knowledge's original-dispatch query) are bound
// to the tenant only, and the stated owner keeps its lookup role.
func dispatchReader(ctx context.Context, tenantID, statedOwner string) (application.Reader, error) {
	p, ok := identity.Caller(ctx)
	if !ok || !(isWorkload(p, modelProxy) || isWorkload(p, mcp)) {
		return application.Reader{TenantID: tenantID}, nil
	}
	if !namesCaller(statedOwner, p) {
		return application.Reader{}, misnamed(statedOwner, p)
	}
	return application.Reader{TenantID: tenantID, Owner: p.ServiceAccount}, nil
}
