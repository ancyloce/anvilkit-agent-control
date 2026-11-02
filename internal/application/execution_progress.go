package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// RecordAttemptProgress records an intra-Job progress fact of the current
// attempt (plan 0005 Phase 2): the trusted coordinator's node boundary, as
// the trusted access sidecar submits it under the attempt, instance and
// execution epoch it confirmed. The record is one operation.changed event
// carrying the attempt and node fields; the attempt, phase, lifecycle,
// budget and verdicts are untouched. The same transition (attempt, node,
// round, state) returns the recorded event; a different outcome code for it
// conflicts. A closed attempt, a non-current instance, a moved epoch or a
// terminal/reconciling operation refuses with STALE_EXECUTION: progress
// never reopens or advances an operation.
func (s *Execution) RecordAttemptProgress(ctx context.Context, cmd domain.CommandIdentity, attemptID, instanceID string, executionEpoch uint64, node, state, round, outcomeCode string) (ev domain.Event, existing bool, err error) {
	if !progressNodes[node] || !progressStates[state] {
		return ev, false, fmt.Errorf("%w: progress node %q state %q", domain.ErrInvalid, node, state)
	}
	if round != "" {
		if _, err := domain.ParseRevision(round); err != nil {
			return ev, false, fmt.Errorf("%w: progress round %q", domain.ErrInvalid, round)
		}
	}
	transition := "progress:" + attemptID + ":" + node + ":" + round + ":" + state
	err = s.store.Tx(ctx, func(r Repo) error {
		op, at, err := lockOperationThenAttempt(ctx, r, attemptID, cmd.TenantID)
		if err != nil {
			return err
		}
		if found, err := r.GetEventByTransition(ctx, op.ID, transition); err == nil {
			if found.Payload.OutcomeCode != outcomeCode {
				return fmt.Errorf("%w: progress %s already recorded with another outcome", domain.ErrIdempotencyConflict, transition)
			}
			ev, existing = *found, true
			return nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		if at.State != domain.AttemptLaunchPrepared && at.State != domain.AttemptRunning {
			return fmt.Errorf("%w: attempt %s is %s; progress needs a running attempt", domain.ErrStaleExecution, at.ID, at.State)
		}
		if at.ExecutionEpoch != executionEpoch || op.ExecutionEpoch != executionEpoch {
			return fmt.Errorf("%w: progress under execution epoch %d, the attempt runs under %d", domain.ErrStaleExecution, executionEpoch, at.ExecutionEpoch)
		}
		if op.Lifecycle.Terminal() || op.Lifecycle == domain.LifecycleReconciling {
			return fmt.Errorf("%w: operation %s is %s", domain.ErrStaleExecution, op.ID, op.Lifecycle)
		}
		inst, err := r.LockInstance(ctx, instanceID)
		if err != nil {
			return err
		}
		if inst.AttemptID != at.ID || !inst.Current {
			return fmt.Errorf("%w: instance %s is not the current instance of attempt %s", domain.ErrStaleExecution, instanceID, at.ID)
		}
		now := s.clock.Now()
		fresh := op.Transition(transition, now, func(*domain.Operation) {})
		fresh.Payload.BindAttempt(at)
		fresh.Payload.BindProgress(node, state, round, outcomeCode)
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		if err := r.InsertEvent(ctx, fresh); err != nil {
			return err
		}
		ev = fresh
		return nil
	})
	return ev, existing, err
}

// progressNodes and progressStates are the reviewed vocabulary of an
// intra-Job progress report (the team graph's nodes and their boundaries).
var (
	progressNodes  = map[string]bool{"planner": true, "retrieve": true, "code": true, "review": true, "validate": true}
	progressStates = map[string]bool{"started": true, "completed": true, "failed": true, "skipped": true}
)
