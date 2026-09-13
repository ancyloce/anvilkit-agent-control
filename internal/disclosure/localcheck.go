package disclosure

import (
	"context"
	"github.com/ancyloce/anvilkit-agent-control/internal/storage"
)

const LocalCheckCreate = "local-check.create"
const OperationCancel = "operation.cancel"

// ComponentPrepare is the action of the preparation routes (S2, 2026-09-13):
// submitting a preparation (no operation yet) and answering its question set
// (an operation this actor owns). Reads and cancellation keep their own actions.
const ComponentPrepare = "component.prepare"

// AuthorizeLocal confines the explicitly enabled fixture scope to the fixed
// local-profile kinds: local checks and, since S2, preparations. A create
// permission never implies either read or cancellation permission.
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
	} else if action == ComponentPrepare {
		resource = "preparation"
		if operationID != "" {
			matched, err := s.store.MatchesLocalCheckScope(ctx, storage.Scope{ActorID: actor.ActorID, TenantID: actor.TenantID}, operationID)
			if err != nil {
				return Decision{}, ErrUnavailable
			}
			if !matched {
				return Decision{}, nil
			}
		}
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
