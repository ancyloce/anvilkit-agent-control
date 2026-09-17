package application

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Operations implements intake, projection, events and tracked commands
// (DD-02 §2). Profiles are the reviewed operation profiles; only their
// kinds can be accepted.
type Operations struct {
	store     Store
	inventory Inventory
	profiles  map[string]domain.Profile
	clock     domain.Clock
	log       *slog.Logger
}

func NewOperations(store Store, inventory Inventory, profiles []domain.Profile, clock domain.Clock, log *slog.Logger) *Operations {
	m := make(map[string]domain.Profile, len(profiles))
	for _, p := range profiles {
		m[p.ID] = p
	}
	return &Operations{store: store, inventory: inventory, profiles: m, clock: clock, log: log}
}

// Profile returns the reviewed profile or ErrProfileUnqualified.
func (s *Operations) Profile(id string) (domain.Profile, error) {
	p, ok := s.profiles[id]
	if !ok {
		return domain.Profile{}, fmt.Errorf("%w: %q", domain.ErrProfileUnqualified, id)
	}
	return p, nil
}

// Create accepts an operation. The record, its dedup key and the initial
// event commit first with a pending intake; the intake obligation is then
// inventoried outside locks and confirmed in a second commit. The caller
// only sees an accepted operation after confirmation; a reentered command
// resumes whichever step was left pending.
//
// A Preparation carries its intake: the prompt must be a finalized prompt
// artifact of the caller's tenant holding the declared digest (a transfer
// bound to no operation, since the operation does not exist yet). A
// Generation binds a brief: the subject's brief must be the current brief
// of a succeeded preparation of the same tenant; a superseded brief starts
// no generation.
func (s *Operations) Create(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, kind domain.OperationKind, subject domain.Subject, intake *domain.PreparationIntake) (op *domain.Operation, existing bool, err error) {
	if scope.TenantID != cmd.TenantID {
		return nil, false, fmt.Errorf("%w: command tenant differs from scope", domain.ErrInvalid)
	}
	profile, err := s.Profile(subject.ProfileID)
	if err != nil {
		return nil, false, err
	}
	switch kind {
	case domain.KindPreparation:
		if intake == nil {
			return nil, false, fmt.Errorf("%w: a preparation needs its intake", domain.ErrInvalid)
		}
	case domain.KindGeneration, domain.KindRefinement:
		if intake != nil {
			return nil, false, fmt.Errorf("%w: only a preparation carries an intake", domain.ErrInvalid)
		}
		if subject.BriefID == "" {
			return nil, false, fmt.Errorf("%w: a %s binds a brief", domain.ErrInvalid, kind)
		}
	default:
		if intake != nil {
			return nil, false, fmt.Errorf("%w: only a preparation carries an intake", domain.ErrInvalid)
		}
	}
	now := s.clock.Now()
	for attempt := 0; attempt < 2; attempt++ {
		err = s.store.Tx(ctx, func(r Repo) error {
			found, err := r.GetOperationByCommand(ctx, cmd.TenantID, cmd.CommandID)
			switch {
			case err == nil:
				if found.SemanticDigest != cmd.RequestDigest {
					return fmt.Errorf("%w: command %s", domain.ErrIdempotencyConflict, cmd.CommandID)
				}
				op, existing = found, true
				return nil
			case !errors.Is(err, domain.ErrNotFound):
				return err
			}
			closed, err := r.IsAdmissionClosed(ctx, scope.TenantID)
			if err != nil {
				return err
			}
			if closed {
				return fmt.Errorf("%w: %s: admission for tenant %s is closed by a recovery run", domain.ErrStaleExecution, domain.DenyRecoveryRestricted, scope.TenantID)
			}
			fresh, err := domain.NewOperation(cmd, scope, kind, subject, profile, now)
			if err != nil {
				return err
			}
			if intake != nil {
				if err := verifyPromptTransfer(ctx, r, scope, intake); err != nil {
					return err
				}
				fresh.Preparation = intake
			}
			if subject.BriefID != "" {
				if err := verifyBriefBinding(ctx, r, scope, subject.BriefID); err != nil {
					return err
				}
			}
			fresh.Revision = 0
			ev := fresh.Transition("accepted", now, func(*domain.Operation) {})
			if err := r.InsertOperation(ctx, fresh); err != nil {
				return err
			}
			if err := r.InsertEvent(ctx, ev); err != nil {
				return err
			}
			op, existing = fresh, false
			return nil
		})
		if errors.Is(err, ErrDuplicateKey) && attempt == 0 {
			continue // raced with another replica on the same command; reload it
		}
		break
	}
	if err != nil {
		return nil, false, err
	}
	if op.Intake == domain.IntakePending {
		if err := s.confirmIntake(ctx, op); err != nil {
			return nil, false, err
		}
	}
	return op, existing, nil
}

// verifyPromptTransfer checks the intake's prompt against the finalized
// transfer it names: the caller's tenant, class prompt, the declared
// digest, bound to no other operation.
func verifyPromptTransfer(ctx context.Context, r Repo, scope domain.Scope, in *domain.PreparationIntake) error {
	t, err := r.GetTransfer(ctx, in.Prompt.TransferID)
	if errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("%w: prompt transfer %s is not a transfer of this scope", domain.ErrInvalid, in.Prompt.TransferID)
	}
	if err != nil {
		return err
	}
	if t.TenantID != scope.TenantID {
		return fmt.Errorf("%w: prompt transfer %s is not a transfer of this scope", domain.ErrInvalid, in.Prompt.TransferID)
	}
	if t.State != domain.TransferFinalized || t.Class != domain.ArtifactPrompt {
		return fmt.Errorf("%w: prompt transfer %s is not a finalized prompt artifact (%s/%s)", domain.ErrInvalid, t.ID, t.State, t.Class)
	}
	if t.ActualDigest != in.Prompt.Digest {
		return fmt.Errorf("%w: prompt transfer %s holds %s, the intake names %s", domain.ErrInvalid, t.ID, t.ActualDigest, in.Prompt.Digest)
	}
	if t.OperationID != "" {
		return fmt.Errorf("%w: prompt transfer %s is bound to operation %s", domain.ErrInvalid, t.ID, t.OperationID)
	}
	return nil
}

// verifyBriefBinding checks that the brief a generation binds is the
// current brief of a succeeded preparation of the caller's tenant.
func verifyBriefBinding(ctx context.Context, r Repo, scope domain.Scope, briefID string) error {
	b, err := r.GetBrief(ctx, briefID)
	if errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("%w: brief %s is not a brief of this scope", domain.ErrInvalid, briefID)
	}
	if err != nil {
		return err
	}
	if err := b.StartsGeneration(scope.TenantID); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("%w: brief %s is not a brief of this scope", domain.ErrInvalid, briefID)
		}
		return err
	}
	prep, err := r.GetOperationScoped(ctx, b.OperationID, scope.TenantID)
	if err != nil {
		return err
	}
	if prep.Lifecycle != domain.LifecycleSucceeded {
		return fmt.Errorf("%w: brief %s belongs to preparation %s which is %s", domain.ErrStaleExecution, briefID, prep.ID, prep.Lifecycle)
	}
	return nil
}

// confirmIntake writes the intake obligation to the independent inventory
// and, only after a durable version comes back, marks the operation
// confirmed and relay-ready. Idempotent under retries and replicas.
func (s *Operations) confirmIntake(ctx context.Context, op *domain.Operation) error {
	key, body := intakeObligation(op)
	version, err := s.inventory.Put(ctx, key, body)
	if err != nil {
		if errors.Is(err, domain.ErrIdempotencyConflict) {
			return fmt.Errorf("%w: intake inventory for %s holds a different record", domain.ErrEffectUncertain, op.ID)
		}
		return fmt.Errorf("%w: intake inventory write for %s: %v", domain.ErrEffectUncertain, op.ID, err)
	}
	now := s.clock.Now()
	return s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockOperation(ctx, op.ID)
		if err != nil {
			return err
		}
		if locked.Intake == domain.IntakeConfirmed {
			*op = *locked
			return nil
		}
		ev := locked.Transition("intake_confirmed", now, func(o *domain.Operation) {
			o.Intake = domain.IntakeConfirmed
			o.IntakeVersion = version
			o.Phase = "accepted"
		})
		if err := r.UpdateOperation(ctx, locked); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		*op = *locked
		return nil
	})
}

// ReconcileIntake resumes intake confirmations left pending by a lost
// acknowledgement or a process failure between the two commits.
func (s *Operations) ReconcileIntake(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	var pending []*domain.Operation
	err := s.store.Read(ctx, func(r Repo) error {
		var err error
		pending, err = r.ListIntakePending(ctx, s.clock.Now().Add(-olderThan), limit)
		return err
	})
	if err != nil {
		return 0, err
	}
	n := 0
	for _, op := range pending {
		if err := s.confirmIntake(ctx, op); err != nil {
			s.log.Warn("intake reconciliation deferred", "operationId", op.ID, "error", err)
			continue
		}
		n++
	}
	return n, nil
}

// Get returns the committed projection within the caller's scope.
func (s *Operations) Get(ctx context.Context, scope domain.Scope, operationID string) (*domain.Operation, error) {
	var op *domain.Operation
	err := s.store.Read(ctx, func(r Repo) error {
		var err error
		op, err = r.GetOperationScoped(ctx, operationID, scope.TenantID)
		return err
	})
	return op, err
}

// EventPage is one page of durable events with the projection's covered
// sequence; ResetRequired means the cursor cannot be honored.
type EventPage struct {
	Events          []domain.Event
	CoveredEventSeq uint64
	ResetRequired   bool
	Terminal        bool
}

// ListEvents reads events after the cursor under current authorization.
func (s *Operations) ListEvents(ctx context.Context, scope domain.Scope, operationID string, afterSeq uint64, limit int) (EventPage, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	var page EventPage
	err := s.store.Read(ctx, func(r Repo) error {
		op, err := r.GetOperationScoped(ctx, operationID, scope.TenantID)
		if err != nil {
			return err
		}
		page.CoveredEventSeq = op.CoveredEventSeq()
		page.Terminal = op.Lifecycle.Terminal()
		if afterSeq > page.CoveredEventSeq {
			page.ResetRequired = true
			return nil
		}
		page.Events, err = r.ListEvents(ctx, operationID, afterSeq, limit)
		return err
	})
	return page, err
}

// SubmitCommand records a tracked control command. Acceptance is not
// application: cancel installs a fence and becomes applied only when the
// senders are known quiescent (no open attempt) or when the Workflow
// closes the attempt later.
func (s *Operations) SubmitCommand(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, operationID string, kind domain.CommandKind, expected domain.Revision, targetActivation string) (c *domain.Command, existing bool, err error) {
	if scope.TenantID != cmd.TenantID {
		return nil, false, fmt.Errorf("%w: command tenant differs from scope", domain.ErrInvalid)
	}
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		found, err := r.GetCommand(ctx, cmd.TenantID, cmd.CommandID)
		switch {
		case err == nil:
			if found.RequestDigest != cmd.RequestDigest || found.OperationID != operationID {
				return fmt.Errorf("%w: command %s", domain.ErrIdempotencyConflict, cmd.CommandID)
			}
			c, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		scoped, err := r.GetOperationScoped(ctx, operationID, scope.TenantID)
		if err != nil {
			return err
		}
		op, err := r.LockOperation(ctx, scoped.ID)
		if err != nil {
			return err
		}
		c = &domain.Command{
			TenantID: cmd.TenantID, CommandID: cmd.CommandID, OperationID: op.ID, ActorID: cmd.ActorID, Kind: kind,
			ExpectedRevision: expected, RequestDigest: cmd.RequestDigest, TargetDefinitionActivation: targetActivation,
			OperationRevision: op.Revision, AcceptedAt: now, Relay: domain.CommandRelayNone,
		}
		if kind != domain.CommandCancel {
			// Hold, resume and change_definition exist for the profiles that
			// support them (Generation); the initial Preparation/LocalCheck
			// profiles support cancellation only (DD-01 §6). A supported
			// command installs its fence first and is relayed as a tracked
			// Update; it becomes applied only when the Workflow's handler
			// answered so (RecordCommandRelay).
			profile, err := s.Profile(op.Subject.ProfileID)
			if err != nil {
				return err
			}
			decision, err := op.DecideControl(profile, kind, expected, targetActivation, now)
			if err != nil {
				return err
			}
			c.Outcome, c.ReasonCode = decision.Outcome, decision.ReasonCode
			if decision.Relay {
				c.Relay = domain.CommandRelayPending
				ev := op.Transition("cmd:"+cmd.CommandID, now, func(o *domain.Operation) {
					if kind == domain.CommandHold {
						o.Control, o.Phase = domain.ControlHoldPending, "hold_pending"
					}
				})
				if err := r.UpdateOperation(ctx, op); err != nil {
					return err
				}
				if err := r.InsertEvent(ctx, ev); err != nil {
					return err
				}
				c.OperationRevision = op.Revision
			}
			if c.Outcome != domain.OutcomePending {
				c.SettledAt = &now
			}
			return r.InsertCommand(ctx, c)
		}
		open, err := r.HasOpenAttempt(ctx, op.ID)
		if err != nil {
			return err
		}
		unresolved, err := unresolvedEffects(ctx, r, op.ID)
		if err != nil {
			return err
		}
		quiescent := op.SendersQuiescent(open || unresolved)
		decision, err := op.DecideCancel(expected, quiescent)
		if err != nil {
			return err
		}
		c.Outcome, c.ReasonCode = decision.Outcome, decision.ReasonCode
		if decision.Applied {
			ev := op.Transition("cmd:"+cmd.CommandID, now, func(o *domain.Operation) {
				o.ApplyCancelFence(quiescent)
				switch o.Relay {
				case domain.RelayPending:
					o.Relay = domain.RelaySettled // never start a canceled operation
				case domain.RelayStarted:
					o.Relay = domain.RelayCancelPending
				}
			})
			if err := r.UpdateOperation(ctx, op); err != nil {
				return err
			}
			if err := r.InsertEvent(ctx, ev); err != nil {
				return err
			}
			c.OperationRevision = op.Revision
		}
		if c.Outcome != domain.OutcomePending {
			c.SettledAt = &now
		}
		return r.InsertCommand(ctx, c)
	})
	if errors.Is(err, ErrDuplicateKey) {
		return s.SubmitCommand(ctx, cmd, scope, operationID, kind, expected, targetActivation)
	}
	return c, existing, err
}

// GetCommand returns the original tracked command within scope.
func (s *Operations) GetCommand(ctx context.Context, scope domain.Scope, operationID, commandID string) (*domain.Command, error) {
	var c *domain.Command
	err := s.store.Read(ctx, func(r Repo) error {
		found, err := r.GetCommand(ctx, scope.TenantID, commandID)
		if err != nil {
			return err
		}
		if found.OperationID != operationID {
			return domain.ErrNotFound
		}
		c = found
		return nil
	})
	return c, err
}

// DigestOf is the canonical request digest helper shared by tests and the
// transport for command bodies.
func DigestOf(body []byte) domain.Digest {
	sum := sha256.Sum256(body)
	return domain.Digest(fmt.Sprintf("sha256:%x", sum))
}
