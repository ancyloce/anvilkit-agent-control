package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Recovery: evidence-bound operator dispositions of unresolved obligations.

// DispositionRequest is one evidence-bound operator decision.
type DispositionRequest struct {
	Class          string
	ObligationID   string
	Decision       domain.DispositionDecision
	EvidenceRef    string
	EvidenceDigest domain.Digest
	RecoveryEpoch  uint64
	RunID          string
	Reason         string
}

// Dispose records an operator disposition of one unresolved obligation:
// the actor's authority for the tenant, the tenant scope of the
// obligation and the evidence object are verified before anything changes
// (the evidence read is external I/O and happens outside every lock); the
// decision then applies to the obligation, the disposition is recorded and
// the run's finding is settled in one commit under the operation lock,
// where the operation's current recovery epoch is checked first. A stale
// or conflicting disposition therefore returns its error with no partial
// change: no effect is resolved, no exposure released, no finding settled.
// A not-sent confirmation releases exposure, an outcome resolution records
// the attested outcome, a retained exposure changes the obligation not at
// all. The same command returns the original disposition; there is no
// bulk form.
func (s *Recovery) Dispose(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, req DispositionRequest) (d *domain.Disposition, existing bool, err error) {
	if err := validClass(req.Class); err != nil {
		return nil, false, err
	}
	allowed := map[string][]domain.DispositionDecision{
		ClassIntake:        {domain.DisposeRetainExposure},
		ClassJobLaunch:     {domain.DisposeRetainExposure},
		ClassModelDispatch: {domain.DisposeConfirmNotSent, domain.DisposeRetainExposure},
		ClassToolDispatch:  {domain.DisposeConfirmNotSent, domain.DisposeRetainExposure},
		ClassBusinessWrite: {domain.DisposeConfirmNotSent, domain.DisposeResolveSucceeded, domain.DisposeResolveFailed, domain.DisposeRetainExposure},
	}
	valid := false
	for _, dec := range allowed[req.Class] {
		valid = valid || dec == req.Decision
	}
	if !valid {
		return nil, false, fmt.Errorf("%w: decision %q does not apply to %s obligations", domain.ErrInvalid, req.Decision, req.Class)
	}
	// Reentry and the obligation's binding, read before any verification.
	// An obligation whose identity the database does not hold and cannot
	// restore (an object that does not decode as its class) has no
	// operation to bind the epoch to: such a finding of the named run is
	// settled against the run's own epoch, and only by retaining exposure.
	var op *domain.Operation
	var run *domain.RecoveryRun
	var dispatch *domain.Dispatch
	if err := s.store.Read(ctx, func(r Repo) error {
		found, err := r.GetDispositionByCommand(ctx, cmd.TenantID, cmd.CommandID)
		switch {
		case err == nil:
			if found.Class != req.Class || found.ObligationID != req.ObligationID || found.Decision != req.Decision || found.EvidenceRef != req.EvidenceRef || found.RequestDigest != cmd.RequestDigest {
				return fmt.Errorf("%w: disposition command %s", domain.ErrIdempotencyConflict, cmd.CommandID)
			}
			d, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		if other, err := r.GetDisposition(ctx, req.Class, req.ObligationID); err == nil {
			return fmt.Errorf("%w: %s %s was disposed by command %s", domain.ErrIdempotencyConflict, req.Class, req.ObligationID, other.CommandID)
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		operationID := ""
		switch req.Class {
		case ClassIntake:
			operationID = req.ObligationID
		case ClassJobLaunch:
			l, err := r.GetLaunch(ctx, req.ObligationID)
			if errors.Is(err, domain.ErrNotFound) {
				return s.unrestorable(ctx, r, cmd, req, &run)
			}
			if err != nil {
				return err
			}
			operationID = l.OperationID
		case ClassModelDispatch, ClassToolDispatch:
			if dispatch, err = r.GetDispatch(ctx, req.ObligationID); errors.Is(err, domain.ErrNotFound) {
				return s.unrestorable(ctx, r, cmd, req, &run)
			} else if err != nil {
				return err
			}
			operationID = dispatch.OperationID
		case ClassBusinessWrite:
			e, err := r.GetEffect(ctx, req.ObligationID)
			if errors.Is(err, domain.ErrNotFound) {
				return s.unrestorable(ctx, r, cmd, req, &run)
			}
			if err != nil {
				return err
			}
			operationID = e.OperationID
		}
		op, err = r.GetOperationScoped(ctx, operationID, cmd.TenantID)
		if errors.Is(err, domain.ErrNotFound) {
			return s.unrestorable(ctx, r, cmd, req, &run)
		}
		return err
	}); err != nil {
		return nil, false, err
	}
	if existing {
		return d, true, nil
	}
	// An early answer for a stale epoch; the authoritative check is made
	// again under the lock before anything is written.
	if op != nil {
		if err := currentRecoveryEpoch(op, req.RecoveryEpoch); err != nil {
			return nil, false, err
		}
	} else if run.RecoveryEpoch != req.RecoveryEpoch {
		return nil, false, fmt.Errorf("%w: disposition names recovery epoch %d, run %s is at epoch %d", domain.ErrStaleExecution, req.RecoveryEpoch, run.ID, run.RecoveryEpoch)
	}
	if err := operator(scope, cmd, cmd.TenantID, ActionDisposition); err != nil {
		return nil, false, err
	}
	if dispatch != nil && req.Decision == domain.DisposeConfirmNotSent {
		if err := s.notSent.VerifyNotSent(ctx, dispatch, req.EvidenceRef, req.EvidenceDigest); err != nil {
			return nil, false, err
		}
	} else if err := s.evidence.VerifyDisposition(ctx, cmd.TenantID, req.RecoveryEpoch, req.Class, req.ObligationID, req.Decision, req.EvidenceRef, req.EvidenceDigest); err != nil {
		return nil, false, err
	}
	// One commit: epoch under the operation lock, the obligation's change,
	// the disposition row and the finding.
	guard := func(locked *domain.Operation) error { return currentRecoveryEpoch(locked, req.RecoveryEpoch) }
	err = s.store.Tx(ctx, func(r Repo) error {
		var lockedOp *domain.Operation
		switch {
		case op == nil:
			// No operation to bind: the run row (rank 1) carries the epoch.
			lockedRun, err := r.LockRecoveryRun(ctx, run.ID)
			if err != nil {
				return err
			}
			if lockedRun.RecoveryEpoch != req.RecoveryEpoch {
				return fmt.Errorf("%w: disposition names recovery epoch %d, run %s is at epoch %d", domain.ErrStaleExecution, req.RecoveryEpoch, lockedRun.ID, lockedRun.RecoveryEpoch)
			}
		case dispatch != nil && req.Decision == domain.DisposeConfirmNotSent:
			// Cost caps and allocations (1), operation (2) and dispatch (4)
			// in rank order; the guard runs under the operation lock before
			// the change.
			if _, _, err := s.dispatch.ConfirmNotSentLocked(ctx, r, op.ID, dispatch.ID, req.EvidenceRef, guard); err != nil {
				return err
			}
		default:
			locked, err := r.LockOperation(ctx, op.ID)
			if err != nil {
				return err
			}
			if err := guard(locked); err != nil {
				return err
			}
			lockedOp = locked
		}
		now := s.clock.Now()
		if req.Class == ClassBusinessWrite && req.Decision != domain.DisposeRetainExposure {
			e, err := r.LockEffect(ctx, req.ObligationID)
			if err != nil {
				return err
			}
			switch req.Decision {
			case domain.DisposeConfirmNotSent:
				if err := e.ConfirmNotSent(req.EvidenceRef); err != nil {
					return err
				}
			default:
				outcome := domain.EffectOutcomeSucceeded
				if req.Decision == domain.DisposeResolveFailed {
					outcome = domain.EffectOutcomeFailed
				}
				if _, err := r.GetEffectObservation(ctx, e.ID, "disposition", req.RecoveryEpoch); errors.Is(err, domain.ErrNotFound) {
					if _, err := e.Observe(outcome, req.EvidenceRef, now); err != nil {
						return err
					}
					if err := r.InsertEffectObservation(ctx, &domain.EffectObservation{ID: domain.NewID("eobs"), EffectID: e.ID, Source: "disposition", Sequence: req.RecoveryEpoch, Outcome: outcome, ReceiptDigest: string(req.EvidenceDigest), ObservedAt: now}); err != nil {
						return err
					}
				} else if err != nil {
					return err
				}
			}
			e.UpdatedAt = now
			if err := r.UpdateEffect(ctx, e); err != nil {
				return err
			}
		}
		_ = lockedOp
		d = &domain.Disposition{
			ID: domain.NewID("dsp"), Class: req.Class, ObligationID: req.ObligationID, TenantID: cmd.TenantID, RunID: req.RunID, RecoveryEpoch: req.RecoveryEpoch,
			CommandID: cmd.CommandID, ActorID: cmd.ActorID, RequestDigest: cmd.RequestDigest, Decision: req.Decision, EvidenceRef: req.EvidenceRef,
			EvidenceDigest: req.EvidenceDigest, Reason: req.Reason, DecidedAt: now,
		}
		if err := r.InsertDisposition(ctx, d); err != nil {
			return err
		}
		if req.RunID == "" {
			return nil
		}
		f, err := r.GetFindingByObligation(ctx, req.RunID, req.Class, req.ObligationID)
		if errors.Is(err, domain.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if lockedF, err := r.LockFinding(ctx, f.ID); err == nil && !lockedF.Status.Settled() {
			lockedF.Status, lockedF.Outcome, lockedF.EvidenceRef, lockedF.Detail, lockedF.UpdatedAt = domain.FindingDisposed, string(req.Decision), req.EvidenceRef, req.Reason, now
			return r.UpdateFinding(ctx, lockedF)
		} else if err != nil {
			return err
		}
		return nil
	})
	if errors.Is(err, ErrDuplicateKey) && depthOf(ctx) < duplicateRetries {
		return s.Dispose(deeper(ctx), cmd, scope, req)
	}
	return d, false, err
}

// unrestorable binds a disposition to the run's finding when the database
// holds no operation for the obligation: the finding must exist in the
// named run, the command must be scoped to the run and the only decision
// that applies is to retain the exposure (nothing else can be established
// about an obligation that cannot be read as its class).
func (s *Recovery) unrestorable(ctx context.Context, r Repo, cmd domain.CommandIdentity, req DispositionRequest, run **domain.RecoveryRun) error {
	if req.RunID == "" {
		return fmt.Errorf("%w: %s %s: no such obligation", domain.ErrNotFound, req.Class, req.ObligationID)
	}
	if req.Decision != domain.DisposeRetainExposure {
		return fmt.Errorf("%w: %s %s has no restorable identity; only retain_exposure applies", domain.ErrInvalid, req.Class, req.ObligationID)
	}
	found, err := r.GetRecoveryRun(ctx, req.RunID)
	if err != nil {
		return err
	}
	if found.ScopeKey != cmd.TenantID {
		return domain.ErrNotFound
	}
	if _, err := r.GetFindingByObligation(ctx, found.ID, req.Class, req.ObligationID); err != nil {
		return err
	}
	*run = found
	return nil
}

// currentRecoveryEpoch refuses a disposition that names another recovery
// epoch than the operation's current one.
func currentRecoveryEpoch(op *domain.Operation, epoch uint64) error {
	if op.RecoveryEpoch != epoch {
		return fmt.Errorf("%w: disposition names recovery epoch %d, operation %s is at epoch %d", domain.ErrStaleExecution, epoch, op.ID, op.RecoveryEpoch)
	}
	return nil
}
