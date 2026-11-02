package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// settleTerminal records what a terminal operation settles beside its
// projection: an applied cancel settles the pending cancel commands and
// every terminal operation releases its execution permit (the pool's
// capacity is freed by the evidence of the close, never by a TTL).
func settleTerminal(ctx context.Context, r Repo, op *domain.Operation, now time.Time) error {
	if op.Lifecycle.Terminal() {
		open, err := r.HasOpenAttempt(ctx, op.ID)
		if err != nil {
			return err
		}
		unresolved, err := unresolvedEffects(ctx, r, op.ID)
		if err != nil {
			return err
		}
		unknownAttempt, err := r.HasUnknownAttempt(ctx, op.ID, "")
		if err != nil {
			return err
		}
		if open || unresolved || unknownAttempt || op.Cleanup == domain.CleanupUnknown || op.Finance == domain.FinanceExposureUnknown {
			return nil
		}
		// Permit locks (rank 5) precede command and event writes (ranks 6/7).
		if err := r.ReleasePermits(ctx, "operation", op.ID, now, "operation:"+string(op.Lifecycle)); err != nil {
			return err
		}
	}
	if op.Control == domain.ControlCancelApplied {
		return r.SettlePendingCommands(ctx, op.ID, domain.CommandCancel, domain.OutcomeApplied, op.Revision, now)
	}
	return nil
}

// obligationsOpen reports whether an obligation of the operation keeps its
// settlement reconciling: an unresolved dispatch or effect, an attempt
// whose cleanup is unknown, an unknown financial exposure or cleanup.
func obligationsOpen(ctx context.Context, r Repo, op *domain.Operation) (unresolved, unknownAttempt bool, err error) {
	if unresolved, err = unresolvedEffects(ctx, r, op.ID); err != nil {
		return false, false, err
	}
	if unknownAttempt, err = r.HasUnknownAttempt(ctx, op.ID, ""); err != nil {
		return false, false, err
	}
	return unresolved || unknownAttempt || op.Finance == domain.FinanceExposureUnknown || op.Cleanup == domain.CleanupUnknown, unknownAttempt, nil
}

// SettleFromIntent re-applies the stored settlement intent of an operation
// Control keeps reconciling, once its last obligation has resolved (P1.6):
// the operation settles as the Workflow stated, under the same terminal
// transition the Workflow's own re-poll would write, whether or not a
// Workflow still polls (a run that reached its history limit, or was
// terminated, still ends with the intended outcome). It changes nothing
// while an obligation is open, for a terminal operation or without an
// intent.
func (s *Execution) SettleFromIntent(ctx context.Context, operationID string) (settled bool, err error) {
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if locked.Lifecycle != domain.LifecycleReconciling || !s.multiStep(locked) {
			return nil
		}
		intent, err := r.GetOperationSettlement(ctx, operationID)
		if errors.Is(err, domain.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		open, err := r.HasOpenAttempt(ctx, locked.ID)
		if err != nil || open {
			return err
		}
		unresolved, _, err := obligationsOpen(ctx, r, locked)
		if err != nil || unresolved {
			return err
		}
		var settle error
		ev := locked.Transition("settle:"+intent.CommandID+":terminal", now, func(o *domain.Operation) {
			settle = o.SettleOperation(intent.Outcome, intent.FailureCode, intent.Phase)
		})
		if settle != nil {
			return settle
		}
		if err := r.UpdateOperation(ctx, locked); err != nil {
			return err
		}
		if err := settleTerminal(ctx, r, locked, now); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		settled = true
		return nil
	})
	if err == nil && settled && s.finance != nil {
		if _, _, ferr := s.finance.SettleFinance(ctx, operationID); ferr != nil {
			s.log.Warn("financial settlement deferred to the sweep", "operationId", operationID, "error", ferr)
		}
	}
	return settled, err
}

// ReconcileSettlements re-applies the stored intents of up to limit
// reconciling operations whose obligations have resolved (P1.6).
func (s *Execution) ReconcileSettlements(ctx context.Context, limit int) (int, error) {
	var ids []string
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		ids, err = r.ListReconcilingSettlements(ctx, limit)
		return err
	}); err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
		settled, err := s.SettleFromIntent(ctx, id)
		if err != nil {
			s.log.Warn("settlement intent not re-applied", "operationId", id, "error", err)
			continue
		}
		if settled {
			n++
		}
	}
	// The financial settlement sweep (P2.1): terminal operations whose
	// money has not settled yet, whatever path ended them.
	if s.finance != nil {
		if m, err := s.finance.ReconcileFinance(ctx, limit); err != nil {
			s.log.Warn("financial settlement sweep failed", "error", err)
		} else if m > 0 {
			s.log.Info("operations settled financially by the sweep", "count", m)
		}
	}
	return n, nil
}

// SettleOperation records the business outcome of a multi-step operation
// (DD-01 §2) under the same command identity: the same command returns the
// original; a terminal operation answers its state; an operation whose
// cleanup is unknown settles through reconciliation instead.
func (s *Execution) SettleOperation(ctx context.Context, cmd domain.CommandIdentity, operationID string, outcome domain.OperationOutcome, failureCode, phase string) (op *domain.Operation, existing bool, err error) {
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if locked.TenantID != cmd.TenantID {
			return domain.ErrNotFound
		}
		if !s.multiStep(locked) {
			return fmt.Errorf("%w: profile %s settles through its attempt close", domain.ErrInvalid, locked.Subject.ProfileID)
		}
		if locked.Lifecycle.Terminal() {
			op, existing = locked, true
			return settleTerminal(ctx, r, locked, now)
		}
		open, err := r.HasOpenAttempt(ctx, locked.ID)
		if err != nil {
			return err
		}
		if open {
			return fmt.Errorf("%w: operation %s still has an open attempt", domain.ErrStaleExecution, locked.ID)
		}
		unresolved, unknownAttempt, err := obligationsOpen(ctx, r, locked)
		if err != nil {
			return err
		}
		intent := &domain.SettlementIntent{OperationID: operationID, CommandID: cmd.CommandID, RequestDigest: cmd.RequestDigest, Outcome: outcome, FailureCode: failureCode, Phase: phase}
		insertIntent := false
		stored, err := r.GetOperationSettlement(ctx, operationID)
		switch {
		case err == nil:
			if *stored != *intent {
				return fmt.Errorf("%w: settlement of %s differs", domain.ErrIdempotencyConflict, operationID)
			}
			if unresolved {
				op, existing = locked, true
				return nil
			}
		case errors.Is(err, domain.ErrNotFound):
			if outcome != domain.OperationSucceeded && outcome != domain.OperationFailed && outcome != domain.OperationCanceled {
				return domain.ErrInvalid
			}
			insertIntent = true
		default:
			return err
		}
		transition := "settle:" + cmd.CommandID + ":terminal"
		if unresolved {
			transition = "settle:" + cmd.CommandID + ":pending"
		}
		var settle error
		ev := locked.Transition(transition, now, func(o *domain.Operation) {
			if unresolved {
				if unknownAttempt {
					o.Cleanup = domain.CleanupUnknown
				}
				// An unresolved obligation (an unknown dispatch or effect)
				// keeps the operation reconciling under the stated phase:
				// settlement continues, it never disappears behind a
				// completed flag (DD-01 §2).
				o.Lifecycle, o.FailureCode = domain.LifecycleReconciling, "EFFECT_UNCERTAIN"
				if phase != "" {
					o.Phase = phase
				}
				return
			}
			settle = o.SettleOperation(outcome, failureCode, phase)
		})
		if settle != nil {
			return settle
		}
		if err := r.UpdateOperation(ctx, locked); err != nil {
			return err
		}
		if err := settleTerminal(ctx, r, locked, now); err != nil {
			return err
		}
		if insertIntent {
			if err := r.InsertOperationSettlement(ctx, intent); err != nil {
				return err
			}
		}
		if err := r.InsertEvent(ctx, ev); err != nil {
			return err
		}
		op = locked
		return nil
	})
	if err == nil {
		op = s.settleFinanceAfter(ctx, op)
	}
	return op, existing, err
}
