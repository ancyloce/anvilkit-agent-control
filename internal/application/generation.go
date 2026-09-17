package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Generations implements the admission and bootstrap facts of a Generation
// (DD-01 §4, DD-02 §2–§3, delivery.md P13-03/06): the bounded execution
// permit that sets the active deadline once, the confirmed lease record,
// the funding occurrence over the shared pools, and the outcomes of the
// relayed control commands. Every mutation runs under the operation lock
// (rank 2), the pool lock (rank 5) when a permit is decided, with the
// clock read after the locks; nothing here touches the network.
type Generations struct {
	store    Store
	dispatch *Dispatch
	profiles map[string]domain.Profile
	pools    []domain.ResourcePool
	clock    domain.Clock
	log      *slog.Logger
}

func NewGenerations(store Store, dispatch *Dispatch, profiles []domain.Profile, pools []domain.ResourcePool, clock domain.Clock, log *slog.Logger) *Generations {
	m := make(map[string]domain.Profile, len(profiles))
	for _, p := range profiles {
		m[p.ID] = p
	}
	return &Generations{store: store, dispatch: dispatch, profiles: m, pools: pools, clock: clock, log: log}
}

// InstallPools writes the reviewed execution pools of this build (their
// capacity is configuration, never a runtime guess); idempotent.
func (s *Generations) InstallPools(ctx context.Context) error {
	return s.store.Tx(ctx, func(r Repo) error {
		for i := range s.pools {
			if err := r.UpsertResourcePool(ctx, &s.pools[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Generations) profile(op *domain.Operation) (domain.Profile, error) {
	p, ok := s.profiles[op.Subject.ProfileID]
	if !ok {
		return domain.Profile{}, fmt.Errorf("%w: %q", domain.ErrProfileUnqualified, op.Subject.ProfileID)
	}
	return p, nil
}

// GenerationView is the admission record of a Generation.
type GenerationView struct {
	Operation *domain.Operation
	Profile   domain.Profile
	Brief     *domain.Brief
	Permit    *domain.Permit
	Funding   *domain.Funding
}

// Get reads the generation inside the tenant.
func (s *Generations) Get(ctx context.Context, tenantID, operationID string) (GenerationView, error) {
	var v GenerationView
	err := s.store.Read(ctx, func(r Repo) error {
		op, err := r.GetOperationScoped(ctx, operationID, tenantID)
		if err != nil {
			return err
		}
		if op.Kind != domain.KindGeneration && op.Kind != domain.KindRefinement {
			return domain.ErrNotFound
		}
		v.Operation = op
		if v.Profile, err = s.profile(op); err != nil {
			return err
		}
		if op.Subject.BriefID != "" {
			if v.Brief, err = r.GetBrief(ctx, op.Subject.BriefID); err != nil {
				return err
			}
		}
		p, err := r.GetPermitByOwner(ctx, "operation", op.ID)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		v.Permit = p
		f, err := r.GetFunding(ctx, op.ID)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		v.Funding = f
		return nil
	})
	return v, err
}

// Operation reads the operation projection inside the tenant (any kind).
func (s *Generations) Operation(ctx context.Context, tenantID, operationID string) (*domain.Operation, error) {
	var op *domain.Operation
	err := s.store.Read(ctx, func(r Repo) error {
		var err error
		op, err = r.GetOperationScoped(ctx, operationID, tenantID)
		return err
	})
	return op, err
}

// PermitAnswer is the answer to a permit request.
type PermitAnswer struct {
	Granted        bool
	Permit         *domain.Permit
	ActiveDeadline *time.Time
	QueueDeadline  time.Time
	QueuedAhead    int64
}

// RequestExecutionPermit decides admission into execution (DD-01 §4). The
// brief the operation binds must still be current; a superseded brief is
// refused before anything is granted. The first grant sets the active
// deadline once; a full pool answers granted=false and the caller asks
// again later under the same command identity (the same digest); a
// different digest under the command conflicts.
func (s *Generations) RequestExecutionPermit(ctx context.Context, cmd domain.CommandIdentity, operationID string) (PermitAnswer, error) {
	var out PermitAnswer
	err := s.store.Tx(ctx, func(r Repo) error {
		op, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if op.TenantID != cmd.TenantID {
			return domain.ErrNotFound
		}
		profile, err := s.profile(op)
		if err != nil {
			return err
		}
		if err := admissionOpen(ctx, r, op.TenantID); err != nil {
			return err
		}
		out.QueueDeadline = op.Deadline
		if op.Subject.BriefID != "" {
			b, err := r.GetBrief(ctx, op.Subject.BriefID)
			if err != nil {
				return err
			}
			if err := b.StartsGeneration(op.TenantID); err != nil {
				return err
			}
		}
		held, err := r.GetActivePermit(ctx, profile.QueuePool, "operation", op.ID)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		var pool *domain.ResourcePool
		var active int64
		if held == nil && profile.QueuePool != "" {
			if pool, err = r.LockResourcePool(ctx, profile.QueuePool); err != nil && !errors.Is(err, domain.ErrNotFound) {
				return err
			}
			if pool != nil {
				if active, err = r.CountActivePermits(ctx, pool.ID); err != nil {
					return err
				}
			}
		}
		now := s.clock.Now()
		decision, err := domain.DecidePermit(op, profile, pool, active, held, now)
		if err != nil {
			return err
		}
		out.Granted, out.Permit, out.ActiveDeadline = decision.Granted, decision.Permit, decision.ActiveDeadline
		if !decision.Granted {
			out.QueuedAhead = active
			if op.Phase != "queued" {
				ev := op.Transition("queue:"+cmd.CommandID, now, func(o *domain.Operation) { o.Phase = "queued" })
				if err := r.UpdateOperation(ctx, op); err != nil {
					return err
				}
				return r.InsertEvent(ctx, ev)
			}
			return nil
		}
		if held != nil {
			return nil // the permit is already the operation's
		}
		if err := r.InsertPermit(ctx, decision.Permit); err != nil {
			return err
		}
		ev := op.Transition("permit:"+decision.Permit.ID, now, func(o *domain.Operation) {
			if o.ActiveDeadline == nil {
				o.ActiveDeadline = decision.ActiveDeadline
			}
			o.Lifecycle, o.Phase = domain.LifecycleRunning, "admitted"
		})
		out.ActiveDeadline = op.ActiveDeadline
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		return r.InsertEvent(ctx, ev)
	})
	if errors.Is(err, ErrDuplicateKey) {
		return s.RequestExecutionPermit(ctx, cmd, operationID)
	}
	return out, err
}

// RecordLease records a confirmed lease report under a stable occurrence.
func (s *Generations) RecordLease(ctx context.Context, cmd domain.CommandIdentity, operationID string, occurrence uint64, state domain.LeaseState, leaseID string, fence uint64, expiresAt *time.Time) (op *domain.Operation, existing bool, err error) {
	err = s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if locked.TenantID != cmd.TenantID {
			return domain.ErrNotFound
		}
		if _, err := s.profile(locked); err != nil {
			return err
		}
		now := s.clock.Now()
		// The rule runs on a copy first: a report that moves nothing (the
		// original's reentry) commits no transition.
		probe := *locked
		changed, err := probe.RecordLease(occurrence, state, leaseID, fence, expiresAt)
		if err != nil {
			return err
		}
		if !changed {
			op, existing = locked, true
			return nil
		}
		ev := locked.Transition("lease:"+cmd.CommandID, now, func(o *domain.Operation) {
			_, _ = o.RecordLease(occurrence, state, leaseID, fence, expiresAt)
			if state == domain.LeaseLost {
				o.Phase = "lease_lost"
			}
		})
		if err := r.UpdateOperation(ctx, locked); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		op = locked
		return nil
	})
	return op, existing, err
}

// RecordFunding allocates the profile's reviewed amount to the operation
// from the current platform/tenant/actor pools under the operation
// identity (Dispatch.Allocate is idempotent per operation); the funding
// occurrence records the command that did it. A profile that funds
// nothing refuses.
func (s *Generations) RecordFunding(ctx context.Context, cmd domain.CommandIdentity, operationID string) (f *domain.Funding, existing bool, err error) {
	var op *domain.Operation
	var profile domain.Profile
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		if op, err = r.GetOperationScoped(ctx, operationID, cmd.TenantID); err != nil {
			return err
		}
		if profile, err = s.profile(op); err != nil {
			return err
		}
		found, err := r.GetFunding(ctx, op.ID)
		switch {
		case err == nil:
			if found.RequestDigest != cmd.RequestDigest || found.CommandID != cmd.CommandID {
				return fmt.Errorf("%w: operation %s is funded under command %s", domain.ErrIdempotencyConflict, op.ID, found.CommandID)
			}
			f, existing = found, true
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		return nil
	}); err != nil {
		return nil, false, err
	}
	if existing {
		return f, true, nil
	}
	if profile.Funding == nil {
		return nil, false, fmt.Errorf("%w: profile %s funds nothing", domain.ErrProfileUnqualified, profile.ID)
	}
	scope := domain.Scope{TenantID: op.TenantID, ProjectID: op.ProjectID, ActorID: op.ActorID}
	if _, _, err := s.dispatch.Allocate(ctx, scope, op.ID, *profile.Funding); err != nil {
		return nil, false, err
	}
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		if _, err := r.LockOperation(ctx, op.ID); err != nil {
			return err
		}
		found, err := r.GetFunding(ctx, op.ID)
		if err == nil {
			f, existing = found, true
			return nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		fresh := &domain.Funding{OperationID: op.ID, CommandID: cmd.CommandID, RequestDigest: cmd.RequestDigest, Amount: *profile.Funding, FundedAt: now}
		if err := r.InsertFunding(ctx, fresh); err != nil {
			return err
		}
		f = fresh
		return nil
	})
	return f, existing, err
}

// RecordCommandRelay records the Workflow handler's answer to a relayed
// hold, resume or change_definition command and applies it to the
// operation: an applied hold suspends, an applied resume runs again, an
// applied change moves the activation. Settled once.
func (s *Generations) RecordCommandRelay(ctx context.Context, tenantID, commandID string, outcome domain.CommandOutcome, reason string) (*domain.Command, error) {
	var c *domain.Command
	now := s.clock.Now()
	err := s.store.Tx(ctx, func(r Repo) error {
		peek, err := r.GetCommand(ctx, tenantID, commandID)
		if err != nil {
			return err
		}
		op, err := r.LockOperation(ctx, peek.OperationID)
		if err != nil {
			return err
		}
		locked, err := r.LockCommand(ctx, tenantID, commandID)
		if err != nil {
			return err
		}
		if locked.Outcome != domain.OutcomePending {
			c = locked
			return nil
		}
		if outcome == domain.OutcomePending {
			return fmt.Errorf("%w: a relay outcome must settle the command", domain.ErrInvalid)
		}
		locked.Outcome, locked.ReasonCode, locked.SettledAt, locked.Relay = outcome, reason, &now, domain.CommandRelaySettled
		ev := op.Transition("cmd:"+commandID+":"+string(outcome), now, func(o *domain.Operation) {
			o.ApplyControl(locked.Kind, outcome, locked.TargetDefinitionActivation)
		})
		locked.OperationRevision = op.Revision
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		if err := r.UpdateCommand(ctx, locked); err != nil {
			return err
		}
		c = locked
		return nil
	})
	return c, err
}

// MarkCommandRelay moves a command's relay intent (pending → sent) without
// settling it; the relay records that the Update was issued and its
// receipt is awaited.
func (s *Generations) MarkCommandRelay(ctx context.Context, tenantID, commandID string, state domain.CommandRelayState) error {
	return s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockCommand(ctx, tenantID, commandID)
		if err != nil {
			return err
		}
		if locked.Outcome != domain.OutcomePending || locked.Relay == domain.CommandRelaySettled {
			return nil
		}
		locked.Relay = state
		return r.UpdateCommand(ctx, locked)
	})
}
