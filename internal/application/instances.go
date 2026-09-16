package application

import (
	"context"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// InstanceScope is what the trusted access sidecar of a Job learns about
// the physical instance it runs in (DD-03 §4, P09): the instance the
// trusted launcher registered from API-server evidence, its attempt, and
// the attempt's operation with the tenant and epochs as of the same read.
// Nothing in it is granted by the caller's own claim: a Pod that the
// launcher has not registered is not found, and an instance that is not
// current carries no authority (every later Control call by that instance
// is refused as stale). The instance row is the historical record of
// physical ownership and is never rewritten to express a decision;
// whether the registration still carries execution authority follows
// from the attempt's state and deadline and from the operation's
// lifecycle, control intent and current execution epoch, exactly the
// facts Control's own transactions check before they accept anything.
type InstanceScope struct {
	Instance      *domain.Instance
	Attempt       *domain.Attempt
	Operation     *domain.Operation
	TenantID      string
	RecoveryEpoch uint64
}

// GetInstance reads the instance registered for one Pod of a launch. The
// launch key must be the one the instance was registered under, so a Pod
// cannot read another launch's registration by guessing a Pod UID.
func (s *Execution) GetInstance(ctx context.Context, backend, launchKey, podUID string) (*InstanceScope, error) {
	var out InstanceScope
	err := s.store.Read(ctx, func(r Repo) error {
		inst, err := r.GetInstanceByPod(ctx, backend, podUID)
		if err != nil {
			return err
		}
		if inst.LaunchKey != launchKey {
			return domain.ErrNotFound
		}
		at, err := r.GetAttempt(ctx, inst.AttemptID)
		if err != nil {
			return err
		}
		op, err := r.GetOperationScoped(ctx, at.OperationID, at.TenantID)
		if err != nil {
			return err
		}
		out = InstanceScope{Instance: inst, Attempt: at, Operation: op, TenantID: op.TenantID, RecoveryEpoch: op.RecoveryEpoch}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}
