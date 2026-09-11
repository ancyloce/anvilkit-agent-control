package disclosure

import (
	"context"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
)

const LocalCheckCreate = "local-check.create"
const OperationCancel = "operation.cancel"

// AuthorizeLocal confines the explicitly enabled fixture scope to local checks.
// A create permission never implies either read or cancellation permission.
func (s *Service) AuthorizeLocal(ctx context.Context, actor Principal, operationID, action string) (Decision, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	b, ok := s.profile.localBindings[actor.ActorID]
	if !ok || b.ActorID != actor.ActorID || b.TenantID != actor.TenantID || b.ServiceIdentity != APIService || !b.ServiceAuthorized {
		return Decision{}, nil
	}
	deadline := earliest(actor.ExpiresAt, b.ValidUntil, s.profile.sourceCredentialExpiresAt)
	if !s.now().Add(clockMargin).Before(deadline) {
		return Decision{}, nil
	}
	resource := "operation"
	if action == LocalCheckCreate {
		if operationID != "" {
			return Decision{}, nil
		}
		resource = "local-check"
	} else {
		if action != OperationRead && action != OperationCancel {
			return Decision{}, nil
		}
		matched, err := s.store.MatchesLocalCheckScope(ctx, storage.Scope{ActorID: actor.ActorID, TenantID: actor.TenantID}, operationID)
		if err != nil {
			return Decision{}, ErrUnavailable
		}
		if !matched {
			return Decision{}, nil
		}
	}
	return s.authorizePermission(ctx, actor, deadline, operationID, resource, action)
}

// LocalPrincipal lets recovery recheck the original fixture's current scope;
// it never reconstructs a credential or trusts actor IDs from a request body.
func (s *Service) LocalPrincipal(scope storage.Scope) (Principal, bool) {
	for _, c := range s.profile.credentials {
		if c.principal.ActorID == scope.ActorID && c.principal.TenantID == scope.TenantID {
			return c.principal, true
		}
	}
	return Principal{}, false
}
