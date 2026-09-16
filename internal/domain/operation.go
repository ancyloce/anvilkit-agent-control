package domain

import (
	"fmt"
	"time"
)

type OperationKind string

const (
	KindPreparation  OperationKind = "preparation"
	KindGeneration   OperationKind = "generation"
	KindRefinement   OperationKind = "refinement"
	KindPreviewBuild OperationKind = "preview_build"
	KindRelease      OperationKind = "release"
	KindLocalCheck   OperationKind = "local_check"
)

type Lifecycle string

const (
	LifecycleAccepted    Lifecycle = "accepted"
	LifecycleRunning     Lifecycle = "running"
	LifecycleWaiting     Lifecycle = "waiting"
	LifecycleReconciling Lifecycle = "reconciling"
	LifecycleSuspended   Lifecycle = "suspended"
	LifecycleSucceeded   Lifecycle = "succeeded"
	LifecycleFailed      Lifecycle = "failed"
	LifecycleCanceled    Lifecycle = "canceled"
)

func (l Lifecycle) Terminal() bool {
	return l == LifecycleSucceeded || l == LifecycleFailed || l == LifecycleCanceled
}

type ControlState string

const (
	ControlNone          ControlState = "none"
	ControlCancelPending ControlState = "cancel_pending"
	ControlCancelApplied ControlState = "cancel_applied"
	ControlHoldPending   ControlState = "hold_pending"
	ControlHoldApplied   ControlState = "hold_applied"
)

type CleanupState string

const (
	CleanupNotRequired CleanupState = "not_required"
	CleanupPending     CleanupState = "pending"
	CleanupComplete    CleanupState = "complete"
	CleanupUnknown     CleanupState = "unknown"
)

type FinanceState string

const (
	FinanceNotFunded       FinanceState = "not_funded"
	FinanceFunded          FinanceState = "funded"
	FinanceSettling        FinanceState = "settling"
	FinanceSettled         FinanceState = "settled"
	FinanceExposureUnknown FinanceState = "exposure_unknown"
)

type IntakeState string

const (
	IntakePending   IntakeState = "pending"
	IntakeConfirmed IntakeState = "confirmed"
)

// RelayState tracks the durable Temporal start/cancel relay intent.
type RelayState string

const (
	RelayPending       RelayState = "pending"
	RelayStarted       RelayState = "started"
	RelayCancelPending RelayState = "cancel_pending"
	RelaySettled       RelayState = "settled"
)

type Subject struct {
	ProfileID      string
	SubjectDigest  Digest
	BriefID        string
	SourceRevision string
}

// Profile is a reviewed operation profile. Only reviewed profiles can be
// accepted; the initial set is fixed by configuration and holds the
// LocalCheck fixture.
type Profile struct {
	ID                string
	Kind              OperationKind
	OperationDeadline time.Duration
	// StepID and JobProfileID bind the single fixed step of this profile.
	StepID       string
	JobProfileID string
}

// Operation is the authoritative record and its public projection.
type Operation struct {
	ID             string
	TenantID       string
	ProjectID      string
	ActorID        string
	CommandID      string
	Kind           OperationKind
	Subject        Subject
	SemanticDigest Digest
	Lifecycle      Lifecycle
	Phase          string
	Control        ControlState
	Cleanup        CleanupState
	Finance        FinanceState
	FailureCode    string
	Revision       Revision
	NextEventSeq   uint64
	ExecutionEpoch uint64
	RecoveryEpoch  uint64
	Deadline       time.Time
	Intake         IntakeState
	IntakeVersion  string
	Relay          RelayState
	RelayRunID     string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CoveredEventSeq is the last committed event sequence of the projection.
func (o *Operation) CoveredEventSeq() uint64 { return o.NextEventSeq - 1 }

// Event is one durable operation event committed with the projection.
type Event struct {
	OperationID  string
	EventSeq     uint64
	TransitionID string
	EventType    string
	Revision     Revision
	Payload      ChangedPayload
	OccurredAt   time.Time
}

// ChangedPayload is the reviewed small payload of operation.changed.
type ChangedPayload struct {
	Lifecycle   Lifecycle    `json:"lifecycle"`
	Phase       string       `json:"phase"`
	Control     ControlState `json:"control"`
	Cleanup     CleanupState `json:"cleanup"`
	Finance     FinanceState `json:"finance"`
	FailureCode string       `json:"failureCode,omitempty"`
}

const EventOperationChanged = "operation.changed"

// NewOperation validates intake inputs against the reviewed profile and
// returns the accepted record with its pending intake state. Nothing here
// touches storage; the application layer commits it.
func NewOperation(cmd CommandIdentity, scope Scope, kind OperationKind, subject Subject, profile Profile, now time.Time) (*Operation, error) {
	if profile.Kind != kind {
		return nil, fmt.Errorf("%w: profile %s is not qualified for kind %s", ErrProfileUnqualified, profile.ID, kind)
	}
	if subject.ProfileID != profile.ID {
		return nil, fmt.Errorf("%w: subject profile %q", ErrProfileUnqualified, subject.ProfileID)
	}
	return &Operation{
		ID:             NewID("op"),
		TenantID:       scope.TenantID,
		ProjectID:      scope.ProjectID,
		ActorID:        scope.ActorID,
		CommandID:      cmd.CommandID,
		Kind:           kind,
		Subject:        subject,
		SemanticDigest: cmd.RequestDigest,
		Lifecycle:      LifecycleAccepted,
		Phase:          "intake",
		Control:        ControlNone,
		Cleanup:        CleanupNotRequired,
		Finance:        FinanceNotFunded,
		Revision:       1,
		NextEventSeq:   1,
		ExecutionEpoch: 1,
		RecoveryEpoch:  1,
		Deadline:       now.Add(profile.OperationDeadline),
		Intake:         IntakePending,
		Relay:          RelayPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// Transition applies a projection change: it bumps the revision, allocates
// the event sequence and returns the event to commit with the row. The
// caller holds the operation lock (rank 2) for the whole transaction.
func (o *Operation) Transition(transitionID string, now time.Time, apply func(*Operation)) Event {
	apply(o)
	o.Revision++
	o.UpdatedAt = now
	ev := Event{
		OperationID:  o.ID,
		EventSeq:     o.NextEventSeq,
		TransitionID: transitionID,
		EventType:    EventOperationChanged,
		Revision:     o.Revision,
		Payload:      o.ChangedPayload(),
		OccurredAt:   now,
	}
	o.NextEventSeq++
	return ev
}

func (o *Operation) ChangedPayload() ChangedPayload {
	return ChangedPayload{Lifecycle: o.Lifecycle, Phase: o.Phase, Control: o.Control, Cleanup: o.Cleanup, Finance: o.Finance, FailureCode: o.FailureCode}
}

// FencedForNewDispatch reports whether new launches or sends are denied:
// any control intent, a terminal lifecycle or unresolved reconciliation
// fences them; issued effects and costs are kept.
func (o *Operation) FencedForNewDispatch() bool {
	return o.Control != ControlNone || o.Lifecycle.Terminal() || o.Lifecycle == LifecycleReconciling
}

// SendersQuiescent reports whether nothing can still act for the operation:
// no open attempt and no unresolved cleanup. Only then may a cancel be
// applied at once; an uncertain cleanup keeps it pending.
func (o *Operation) SendersQuiescent(openAttempt bool) bool {
	return !openAttempt && o.Lifecycle != LifecycleReconciling && o.Cleanup != CleanupUnknown
}

type CommandKind string

const (
	CommandCancel           CommandKind = "cancel"
	CommandHold             CommandKind = "hold"
	CommandResume           CommandKind = "resume"
	CommandChangeDefinition CommandKind = "change_definition"
)

type CommandOutcome string

const (
	OutcomePending  CommandOutcome = "pending"
	OutcomeApplied  CommandOutcome = "applied"
	OutcomeBlocked  CommandOutcome = "blocked"
	OutcomeRejected CommandOutcome = "rejected"
)

// Command is a tracked control command and its receipt.
type Command struct {
	TenantID                   string
	CommandID                  string
	OperationID                string
	ActorID                    string
	Kind                       CommandKind
	ExpectedRevision           Revision
	RequestDigest              Digest
	TargetDefinitionActivation string
	Outcome                    CommandOutcome
	ReasonCode                 string
	OperationRevision          Revision
	AcceptedAt                 time.Time
	SettledAt                  *time.Time
}

// CancelDecision is the pure outcome of a cancel request against the
// current operation state; sendersQuiescent (SendersQuiescent) is true when
// nothing can still act, so the fence alone makes cancellation applied.
type CancelDecision struct {
	Outcome    CommandOutcome
	ReasonCode string
	Applied    bool // the operation itself transitions in this commit
}

// DecideCancel applies M1-14: a stale expected revision conflicts, a
// terminal operation rejects, repeated cancel reports the current state,
// and an active operation installs the fence first.
func (o *Operation) DecideCancel(expected Revision, sendersQuiescent bool) (CancelDecision, error) {
	if expected != o.Revision {
		return CancelDecision{}, fmt.Errorf("%w: expected %d, current %d", ErrRevisionConflict, expected, o.Revision)
	}
	switch {
	case o.Lifecycle.Terminal():
		return CancelDecision{Outcome: OutcomeRejected, ReasonCode: "TERMINAL"}, nil
	case o.Control == ControlCancelApplied:
		return CancelDecision{Outcome: OutcomeApplied}, nil
	case o.Control == ControlCancelPending:
		return CancelDecision{Outcome: OutcomePending}, nil
	case sendersQuiescent:
		return CancelDecision{Outcome: OutcomeApplied, Applied: true}, nil
	default:
		return CancelDecision{Outcome: OutcomePending, Applied: true}, nil
	}
}

// ApplyCancelFence installs the cancel fence; when quiescent the operation
// becomes canceled in the same commit.
func (o *Operation) ApplyCancelFence(sendersQuiescent bool) {
	if sendersQuiescent {
		o.Control = ControlCancelApplied
		o.Lifecycle = LifecycleCanceled
		o.Phase = "canceled"
		if o.Cleanup == CleanupPending {
			o.Cleanup = CleanupComplete
		}
		return
	}
	o.Control = ControlCancelPending
	o.Phase = "cancel_pending"
}
