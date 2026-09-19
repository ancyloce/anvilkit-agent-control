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

// ArtifactBinding names a finalized transfer and the digest it must hold.
type ArtifactBinding struct {
	TransferID string `json:"transferId"`
	Digest     Digest `json:"digest"`
}

// SourceReference names an exact revision of a Knowledge source.
type SourceReference struct {
	SourceID string `json:"sourceId"`
	Revision uint64 `json:"revision,string"`
}

// PreparationIntake is the accepted input of a Preparation (API-01).
type PreparationIntake struct {
	Prompt          ArtifactBinding
	BrandReferences []SourceReference
	AssetReferences []SourceReference
}

// ClarificationBounds are the reviewed trial defaults of grouped
// clarification (requirements.md §2): rounds, questions per round and the
// absolute wait per round. Configuration may change future defaults; an
// active question set keeps the expiry it was recorded with.
type ClarificationBounds struct {
	MaxRounds    uint64
	MaxQuestions uint64
	Wait         time.Duration
}

// Profile is a reviewed operation profile. Only reviewed profiles can be
// accepted; the initial set is fixed by configuration and holds the
// LocalCheck fixture, the Preparation profile and the Generation profile.
type Profile struct {
	ID                string
	Kind              OperationKind
	OperationDeadline time.Duration
	// StepID and JobProfileID bind the single fixed step of this profile.
	StepID       string
	JobProfileID string
	// MultiStep profiles (Preparation, Generation) settle their business
	// outcome through SettleOperation; a definite attempt close does not
	// end them (DD-01 §2).
	MultiStep bool
	// Clarification bounds a Preparation's grouped question rounds.
	Clarification ClarificationBounds
	// Funding is the reviewed amount a Generation allocates from the shared
	// pools at bootstrap; nil funds nothing (a Preparation's analysis calls
	// still need an allocation, so its profile funds its analysis
	// allowance).
	Funding *Money
	// QueuePool names the execution capacity pool a Generation waits on;
	// empty means no execution queue (the intake deadline is the only one).
	QueuePool string
	// ActiveWindow is the execution window the first permit opens once.
	ActiveWindow time.Duration
	// Definitions are the reviewed definition activations of the profile;
	// the first is the default, a tracked change moves to another.
	Definitions []string
	// SupportsControl says the profile supports hold, resume and
	// change_definition; the initial Preparation/LocalCheck profiles do not.
	SupportsControl bool
	// MaxRepairs bounds the independently classified repair rounds of a
	// Generation; CodegenProfileID and ValidatorProfileID are the reviewed
	// job profiles its steps launch.
	MaxRepairs         uint64
	CodegenProfileID   string
	ValidatorProfileID string
}

// DefaultDefinition is the activation a fresh operation of the profile runs under.
func (p Profile) DefaultDefinition() string {
	if len(p.Definitions) == 0 {
		return ""
	}
	return p.Definitions[0]
}

// HasDefinition reports whether the activation is one of the profile's.
func (p Profile) HasDefinition(activation string) bool {
	for _, d := range p.Definitions {
		if d == activation {
			return true
		}
	}
	return false
}

// LeaseState is what the protected supervisor confirmed about the source
// lease of a Generation (DD-01 §4): only confirmed results are recorded.
type LeaseState string

const (
	LeaseNone     LeaseState = "none"
	LeaseHeld     LeaseState = "held"
	LeaseLost     LeaseState = "lost"
	LeaseReleased LeaseState = "released"
)

// LeaseRecord is the last confirmed lease fact of an operation.
type LeaseRecord struct {
	State      LeaseState
	LeaseID    string
	Fence      uint64
	ExpiresAt  *time.Time
	Occurrence uint64
}

// Valid reports whether the confirmed lease covers the instant: held and
// its known expiry not passed. An unknown renewal never moves the expiry,
// so validity ends when the last confirmed one does.
func (l LeaseRecord) Valid(now time.Time) bool {
	return l.State == LeaseHeld && l.ExpiresAt != nil && now.Before(*l.ExpiresAt)
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
	// ActiveDeadline is set once by the first execution permit (DD-01 §4);
	// nil until then. Deadline stays the original queue deadline.
	ActiveDeadline *time.Time
	// Lease is the last confirmed lease fact; a lost lease fences new
	// dispatch for good.
	Lease LeaseRecord
	// DefinitionActivation is the reviewed definition the operation runs
	// under; a tracked change_definition moves it.
	DefinitionActivation string
	// Preparation is the accepted intake of a Preparation (nil otherwise).
	Preparation *PreparationIntake
	// CandidateEffectID names the registered candidate's effect once a
	// Generation reached candidate_ready.
	CandidateEffectID string
}

// EffectiveDeadline is the absolute execution deadline: the active
// deadline once a permit set it, otherwise the intake deadline.
func (o *Operation) EffectiveDeadline() time.Time {
	if o.ActiveDeadline != nil {
		return *o.ActiveDeadline
	}
	return o.Deadline
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
		ID:                   NewID("op"),
		TenantID:             scope.TenantID,
		ProjectID:            scope.ProjectID,
		ActorID:              scope.ActorID,
		CommandID:            cmd.CommandID,
		Kind:                 kind,
		Subject:              subject,
		SemanticDigest:       cmd.RequestDigest,
		Lifecycle:            LifecycleAccepted,
		Phase:                "intake",
		Control:              ControlNone,
		Cleanup:              CleanupNotRequired,
		Finance:              FinanceNotFunded,
		Revision:             1,
		NextEventSeq:         1,
		ExecutionEpoch:       1,
		RecoveryEpoch:        1,
		Deadline:             now.Add(profile.OperationDeadline),
		Intake:               IntakePending,
		Relay:                RelayPending,
		CreatedAt:            now,
		UpdatedAt:            now,
		Lease:                LeaseRecord{State: LeaseNone},
		DefinitionActivation: profile.DefaultDefinition(),
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
	return o.Control != ControlNone || o.Lifecycle.Terminal() || o.Lifecycle == LifecycleReconciling || o.Lease.State == LeaseLost
}

// SendersQuiescent reports whether nothing can still act for the operation:
// no open attempt and no unresolved cleanup. Only then may a cancel be
// applied at once; an uncertain cleanup keeps it pending.
func (o *Operation) SendersQuiescent(openAttempt bool) bool {
	return !openAttempt && o.Lifecycle != LifecycleReconciling && o.Cleanup != CleanupUnknown && o.Finance != FinanceExposureUnknown
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
	// Relay is the durable Update relay intent of a hold, resume or
	// change_definition command: pending until issued, sent while its
	// receipt is awaited, settled once the outcome is recorded.
	Relay CommandRelayState
}

// CommandRelayState tracks the tracked Update of a control command.
type CommandRelayState string

const (
	CommandRelayNone    CommandRelayState = "none"
	CommandRelayPending CommandRelayState = "pending"
	CommandRelaySent    CommandRelayState = "sent"
	CommandRelaySettled CommandRelayState = "settled"
)

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
