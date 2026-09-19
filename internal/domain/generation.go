package domain

import (
	"fmt"
	"time"
)

// Generation admission records (DD-01 §4, DD-02 §2): the execution permit
// that sets the active deadline once, the confirmed lease facts, the
// funding occurrence and the supported control commands of a Generation.

type PermitState string

const (
	PermitActive             PermitState = "active"
	PermitReleased           PermitState = "released"
	PermitExpiredUnconfirmed PermitState = "expired_unconfirmed"
)

// ResourcePool is one bounded execution capacity (contracts.md §2
// resource_pools): capacity permits at once, reserved_control of them kept
// for control work.
type ResourcePool struct {
	ID              string
	Class           string
	Capacity        int32
	ReservedControl int32
}

// Available reports whether another business permit fits beside the
// active ones.
func (p *ResourcePool) Available(active int64) bool {
	return active < int64(p.Capacity-p.ReservedControl)
}

// Permit is one operation's execution permission in a pool (owner_kind
// operation): unique while active.
type Permit struct {
	ID              string
	PoolID          string
	OwnerKind       string
	OwnerID         string
	FenceEpoch      uint64
	State           PermitState
	GrantedAt       time.Time
	ReleasedAt      *time.Time
	ReleaseEvidence string
}

// PermitDecision is the pure outcome of an execution permit request.
type PermitDecision struct {
	Granted bool
	Permit  *Permit
	// ActiveDeadline is the operation's active deadline after the decision
	// (set by this grant when it was nil).
	ActiveDeadline *time.Time
}

// DecidePermit admits a Generation into execution (DD-01 §4): a fenced,
// terminal or unfunded-brief operation is refused; a passed queue deadline
// is refused (the original queue deadline is never extended); an operation
// already holding its permit keeps it; otherwise a permit is granted when
// the pool has room, and the first grant sets the active deadline once —
// a later request, retry, hold or replacement never resets it.
func DecidePermit(op *Operation, profile Profile, pool *ResourcePool, active int64, held *Permit, now time.Time) (PermitDecision, error) {
	if op.Kind != KindGeneration && op.Kind != KindRefinement {
		return PermitDecision{}, fmt.Errorf("%w: operation %s is a %s", ErrInvalid, op.ID, op.Kind)
	}
	if op.FencedForNewDispatch() {
		return PermitDecision{}, fmt.Errorf("%w: operation %s is fenced (%s/%s)", ErrStaleExecution, op.ID, op.Lifecycle, op.Control)
	}
	if op.Intake != IntakeConfirmed {
		return PermitDecision{}, fmt.Errorf("%w: intake of %s is not confirmed", ErrStaleExecution, op.ID)
	}
	if held != nil && held.State == PermitActive {
		return PermitDecision{Granted: true, Permit: held, ActiveDeadline: op.ActiveDeadline}, nil
	}
	if !now.Before(op.Deadline) {
		return PermitDecision{}, fmt.Errorf("%w: operation %s queue deadline %s passed", ErrStaleExecution, op.ID, op.Deadline.UTC().Format(time.RFC3339))
	}
	if pool == nil || pool.ID != profile.QueuePool {
		return PermitDecision{}, fmt.Errorf("%w: profile %s names no execution pool of this build", ErrProfileUnqualified, profile.ID)
	}
	if !pool.Available(active) {
		return PermitDecision{Granted: false, ActiveDeadline: op.ActiveDeadline}, nil
	}
	permit := &Permit{ID: NewID("prm"), PoolID: pool.ID, OwnerKind: "operation", OwnerID: op.ID, FenceEpoch: op.ExecutionEpoch, State: PermitActive, GrantedAt: now}
	deadline := op.ActiveDeadline
	if deadline == nil {
		d := now.UTC().Truncate(time.Microsecond).Add(profile.ActiveWindow)
		deadline = &d
	}
	return PermitDecision{Granted: true, Permit: permit, ActiveDeadline: deadline}, nil
}

// Funding is the funding occurrence of an operation: the reviewed amount
// allocated once under the operation identity.
type Funding struct {
	OperationID   string
	CommandID     string
	RequestDigest Digest
	Amount        Money
	FundedAt      time.Time
}

// RecordLease applies a confirmed lease report under a stable occurrence
// (DD-01 §4): a report older than the last recorded one is the original's
// reentry (ignored, existing); held or renewed moves the known expiry only
// forward from a confirmed value; lost fences the operation for good;
// released ends the lease. An operation whose lease was lost never returns
// to held.
func (o *Operation) RecordLease(occurrence uint64, state LeaseState, leaseID string, fence uint64, expiresAt *time.Time) (changed bool, err error) {
	if occurrence == 0 {
		return false, fmt.Errorf("%w: lease occurrence must be positive", ErrInvalid)
	}
	if occurrence <= o.Lease.Occurrence {
		return false, nil
	}
	switch state {
	case LeaseHeld:
		if o.Lease.State == LeaseLost {
			return false, fmt.Errorf("%w: the lease of operation %s was lost; new business work needs a new operation", ErrStaleExecution, o.ID)
		}
		if leaseID == "" || expiresAt == nil {
			return false, fmt.Errorf("%w: a held lease needs its identity and confirmed expiry", ErrInvalid)
		}
		if o.Lease.State == LeaseHeld && o.Lease.LeaseID != leaseID {
			return false, fmt.Errorf("%w: operation %s holds lease %s, the report names %s", ErrInvalid, o.ID, o.Lease.LeaseID, leaseID)
		}
		exp := expiresAt.UTC().Truncate(time.Microsecond)
		if o.Lease.ExpiresAt != nil && exp.Before(*o.Lease.ExpiresAt) {
			exp = *o.Lease.ExpiresAt // a confirmed expiry never moves back
		}
		o.Lease = LeaseRecord{State: LeaseHeld, LeaseID: leaseID, Fence: fence, ExpiresAt: &exp, Occurrence: occurrence}
	case LeaseLost:
		o.Lease = LeaseRecord{State: LeaseLost, LeaseID: o.Lease.LeaseID, Fence: o.Lease.Fence, ExpiresAt: o.Lease.ExpiresAt, Occurrence: occurrence}
	case LeaseReleased:
		if o.Lease.State == LeaseLost {
			o.Lease.Occurrence = occurrence
			return true, nil
		}
		o.Lease = LeaseRecord{State: LeaseReleased, LeaseID: o.Lease.LeaseID, Fence: o.Lease.Fence, ExpiresAt: o.Lease.ExpiresAt, Occurrence: occurrence}
	default:
		return false, fmt.Errorf("%w: lease state %q", ErrInvalid, state)
	}
	return true, nil
}

// ControlDecision is the pure outcome of a hold, resume or change_definition
// request: the receipt outcome and whether a relay to the Workflow follows.
type ControlDecision struct {
	Outcome    CommandOutcome
	ReasonCode string
	Relay      bool
}

// DecideControl applies M1-14 to the supported commands of a profile
// (DD-01 §6): a stale expected revision conflicts; an unsupported profile
// rejects; a terminal operation rejects; a cancel already installed
// dominates; hold installs the fence first (hold_pending) and is relayed;
// resume needs an applied hold, a lease that is not lost and an active
// deadline not passed, and is relayed; change_definition needs a reviewed
// activation of the profile other than the current one and is relayed.
// Nothing here resets a deadline, a budget or an accepted effect.
func (o *Operation) DecideControl(profile Profile, kind CommandKind, expected Revision, target string, now time.Time) (ControlDecision, error) {
	if expected != o.Revision {
		return ControlDecision{}, fmt.Errorf("%w: expected %d, current %d", ErrRevisionConflict, expected, o.Revision)
	}
	if !profile.SupportsControl {
		return ControlDecision{Outcome: OutcomeRejected, ReasonCode: "UNSUPPORTED_BY_PROFILE"}, nil
	}
	if o.Lifecycle.Terminal() {
		return ControlDecision{Outcome: OutcomeRejected, ReasonCode: "TERMINAL"}, nil
	}
	if o.Control == ControlCancelPending || o.Control == ControlCancelApplied {
		return ControlDecision{Outcome: OutcomeRejected, ReasonCode: "CANCEL_INSTALLED"}, nil
	}
	if o.Lifecycle == LifecycleReconciling {
		return ControlDecision{Outcome: OutcomeBlocked, ReasonCode: "RECONCILING"}, nil
	}
	switch kind {
	case CommandHold:
		switch o.Control {
		case ControlHoldApplied:
			return ControlDecision{Outcome: OutcomeApplied}, nil
		case ControlHoldPending:
			return ControlDecision{Outcome: OutcomePending}, nil
		}
		return ControlDecision{Outcome: OutcomePending, Relay: true}, nil
	case CommandResume:
		switch o.Control {
		case ControlNone:
			return ControlDecision{Outcome: OutcomeRejected, ReasonCode: "NOT_HELD"}, nil
		case ControlHoldPending:
			return ControlDecision{Outcome: OutcomeBlocked, ReasonCode: "HOLD_PENDING"}, nil
		}
		if o.Lease.State == LeaseLost {
			return ControlDecision{Outcome: OutcomeRejected, ReasonCode: "LEASE_LOST"}, nil
		}
		if !now.Before(o.EffectiveDeadline()) {
			return ControlDecision{Outcome: OutcomeRejected, ReasonCode: "DEADLINE_EXCEEDED"}, nil
		}
		return ControlDecision{Outcome: OutcomePending, Relay: true}, nil
	case CommandChangeDefinition:
		if target == "" || !profile.HasDefinition(target) {
			return ControlDecision{Outcome: OutcomeRejected, ReasonCode: "DEFINITION_UNKNOWN"}, nil
		}
		if target == o.DefinitionActivation {
			return ControlDecision{Outcome: OutcomeApplied}, nil
		}
		if o.Control == ControlHoldPending {
			return ControlDecision{Outcome: OutcomeBlocked, ReasonCode: "HOLD_PENDING"}, nil
		}
		return ControlDecision{Outcome: OutcomePending, Relay: true}, nil
	}
	return ControlDecision{Outcome: OutcomeRejected, ReasonCode: "UNSUPPORTED_BY_PROFILE"}, nil
}

// ApplyControl records the Workflow's answer to a relayed command: an
// applied hold suspends, an applied resume runs again, an applied
// change_definition moves the activation; a rejected or blocked hold
// removes the pending fence, a rejected resume keeps the hold. Deadlines,
// budgets and effects are untouched.
func (o *Operation) ApplyControl(kind CommandKind, outcome CommandOutcome, target string) {
	// Receipts can arrive after a newer command. Cancellation and terminal
	// state dominate every late receipt, including a definition change.
	if o.Lifecycle.Terminal() || o.Control == ControlCancelPending || o.Control == ControlCancelApplied {
		return
	}
	switch kind {
	case CommandHold:
		if outcome == OutcomeApplied {
			o.Control, o.Lifecycle, o.Phase = ControlHoldApplied, LifecycleSuspended, "held"
		} else if o.Control == ControlHoldPending {
			o.Control = ControlNone
			if o.Phase == "hold_pending" {
				o.Phase = "running"
			}
		}
	case CommandResume:
		if outcome == OutcomeApplied && o.Control == ControlHoldApplied {
			o.Control, o.Lifecycle, o.Phase = ControlNone, LifecycleRunning, "resumed"
		}
	case CommandChangeDefinition:
		if outcome == OutcomeApplied {
			o.DefinitionActivation = target
		}
	}
}

// OperationOutcome is the business outcome a multi-step Workflow settles.
type OperationOutcome string

const (
	OperationSucceeded OperationOutcome = "succeeded"
	OperationFailed    OperationOutcome = "failed"
	OperationCanceled  OperationOutcome = "canceled"
)

// SettleOperation applies the business outcome of a multi-step operation
// (DD-01 §2): a cancel intent dominates a success; the cleanup of the
// operation is what its attempts recorded (an unknown cleanup keeps the
// operation reconciling instead of terminal); the relay settles.
func (o *Operation) SettleOperation(outcome OperationOutcome, failureCode, phase string) error {
	if o.Lifecycle.Terminal() {
		return nil
	}
	if o.Cleanup == CleanupUnknown {
		return fmt.Errorf("%w: operation %s has unknown cleanup; it settles through reconciliation", ErrStaleExecution, o.ID)
	}
	cancelRequested := o.Control == ControlCancelPending || o.Control == ControlCancelApplied || outcome == OperationCanceled
	if phase != "" {
		o.Phase = phase
	}
	switch {
	case cancelRequested:
		o.Control, o.Lifecycle, o.FailureCode = ControlCancelApplied, LifecycleCanceled, ""
		if phase == "" {
			o.Phase = "canceled"
		}
	case outcome == OperationSucceeded:
		o.Lifecycle, o.FailureCode = LifecycleSucceeded, ""
	default:
		o.Lifecycle = LifecycleFailed
		o.FailureCode = firstNonEmpty(failureCode, o.FailureCode, "FAILED")
	}
	o.Relay = RelaySettled
	return nil
}

// SettlementIntent freezes the requested business outcome across reconciliation.
type SettlementIntent struct {
	OperationID   string
	CommandID     string
	RequestDigest Digest
	Outcome       OperationOutcome
	FailureCode   string
	Phase         string
}
