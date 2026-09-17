package domain

import (
	"fmt"
	"time"
)

type AttemptState string

const (
	AttemptOpen           AttemptState = "open"
	AttemptLaunchPrepared AttemptState = "launch_prepared"
	AttemptRunning        AttemptState = "running"
	AttemptResultAccepted AttemptState = "result_accepted"
	AttemptClosed         AttemptState = "closed"
)

type AttemptOutcome string

const (
	OutcomeCompleted            AttemptOutcome = "completed"
	OutcomeFailed               AttemptOutcome = "failed"
	OutcomeInfrastructureFailed AttemptOutcome = "infrastructure_failed"
	OutcomeCanceled             AttemptOutcome = "canceled"
	OutcomeUnknown              AttemptOutcome = "unknown"
)

// Attempt is a logical execution allocation of one step visit.
type Attempt struct {
	ID              string
	OperationID     string
	TenantID        string
	StepID          string
	VisitOrdinal    uint64
	AttemptOrdinal  uint64
	ProfileID       string
	ExecutionEpoch  uint64
	CommandID       string
	RequestDigest   Digest
	State           AttemptState
	Outcome         AttemptOutcome
	Cleanup         CleanupState
	FailureCode     string
	AcceptedStageID string
	Deadline        time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// NewAttempt opens an attempt under the operation's current execution epoch.
// A fenced or terminal operation cannot open attempts.
func NewAttempt(op *Operation, cmd CommandIdentity, stepID string, visit uint64, ordinal uint64, profileID string, now time.Time) (*Attempt, error) {
	if op.FencedForNewDispatch() {
		return nil, fmt.Errorf("%w: operation %s is fenced (%s/%s)", ErrStaleExecution, op.ID, op.Lifecycle, op.Control)
	}
	if op.Intake != IntakeConfirmed {
		return nil, fmt.Errorf("%w: intake of %s is not confirmed", ErrStaleExecution, op.ID)
	}
	// The attempt inherits the absolute deadline: the active deadline the
	// first execution permit set once, or the intake deadline; a retry
	// never opens a later one.
	deadline := op.EffectiveDeadline()
	if !now.Before(deadline) {
		return nil, fmt.Errorf("%w: operation %s deadline %s passed", ErrStaleExecution, op.ID, deadline.UTC().Format(time.RFC3339))
	}
	return &Attempt{
		ID: NewID("att"), OperationID: op.ID, TenantID: op.TenantID, StepID: stepID, VisitOrdinal: visit,
		AttemptOrdinal: ordinal, ProfileID: profileID, ExecutionEpoch: op.ExecutionEpoch,
		CommandID: cmd.CommandID, RequestDigest: cmd.RequestDigest, State: AttemptOpen,
		Deadline: deadline, CreatedAt: now, UpdatedAt: now,
	}, nil
}

type InventoryState string

const (
	InventoryPending   InventoryState = "pending"
	InventoryConfirmed InventoryState = "confirmed"
	InventoryUncertain InventoryState = "uncertain"
)

// Launch is the job-launch obligation persisted before any Kubernetes create.
type Launch struct {
	ID               string
	AttemptID        string
	OperationID      string
	LaunchKey        string
	Backend          string
	ProfileID        string
	ImageDigest      Digest
	ExecutionEpoch   uint64
	LaunchEpoch      uint64
	Deadline         time.Time
	CommandID        string
	RequestDigest    Digest
	Inventory        InventoryState
	InventoryVersion string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// NewLaunch records the obligation; the attempt must be open under the
// current epoch, the operation must not be fenced and the absolute deadline
// set at intake must not have passed: no Job is launched after it, not even
// by a retry of an earlier command.
func NewLaunch(op *Operation, at *Attempt, cmd CommandIdentity, launchKey, backend string, image Digest, deadline time.Time, now time.Time) (*Launch, error) {
	if op.FencedForNewDispatch() {
		return nil, fmt.Errorf("%w: operation %s is fenced", ErrStaleExecution, op.ID)
	}
	if at.State != AttemptOpen && at.State != AttemptLaunchPrepared {
		return nil, fmt.Errorf("%w: attempt %s is %s", ErrStaleExecution, at.ID, at.State)
	}
	if at.ExecutionEpoch != op.ExecutionEpoch {
		return nil, fmt.Errorf("%w: attempt epoch %d, operation epoch %d", ErrStaleExecution, at.ExecutionEpoch, op.ExecutionEpoch)
	}
	if !now.Before(at.Deadline) {
		return nil, fmt.Errorf("%w: attempt %s deadline %s passed", ErrStaleExecution, at.ID, at.Deadline.UTC().Format(time.RFC3339))
	}
	// The stored precision is the identity: the inventory body of this
	// launch must serialize identically from the in-memory record and from
	// the row a retrying replica reloads (SystemClock).
	deadline = deadline.UTC().Truncate(time.Microsecond)
	if deadline.After(at.Deadline) {
		deadline = at.Deadline // a launch never extends the original deadline
	}
	if !deadline.After(now) {
		return nil, fmt.Errorf("%w: launch deadline %s already passed", ErrStaleExecution, deadline.UTC().Format(time.RFC3339))
	}
	return &Launch{
		ID: NewID("lch"), AttemptID: at.ID, OperationID: op.ID, LaunchKey: launchKey, Backend: backend,
		ProfileID: at.ProfileID, ImageDigest: image, ExecutionEpoch: op.ExecutionEpoch, LaunchEpoch: 1,
		Deadline: deadline, CommandID: cmd.CommandID, RequestDigest: cmd.RequestDigest,
		Inventory: InventoryPending, CreatedAt: now, UpdatedAt: now,
	}, nil
}

type InstancePhase string

const (
	PhasePending   InstancePhase = "pending"
	PhaseRunning   InstancePhase = "running"
	PhaseSucceeded InstancePhase = "succeeded"
	PhaseFailed    InstancePhase = "failed"
	PhaseUnknown   InstancePhase = "unknown"
)

// Instance is an actual launched Pod observed by the trusted launcher.
type Instance struct {
	ID           string
	AttemptID    string
	LaunchID     string
	LaunchKey    string
	Backend      string
	JobUID       string
	PodUID       string
	ImageDigest  Digest
	LaunchEpoch  uint64
	Phase        InstancePhase
	ExitCode     *int32
	Current      bool
	RegisteredAt time.Time
	ObservedAt   *time.Time
}

// NewInstance registers real backend evidence. The first instance of a
// launch becomes the current physical owner; any later Pod is recorded but
// never current, so its results are rejected.
func NewInstance(l *Launch, jobUID, podUID string, image Digest, hasCurrent bool, now time.Time) (*Instance, error) {
	if image != l.ImageDigest {
		return nil, fmt.Errorf("%w: observed image %s differs from launched %s", ErrStaleExecution, image, l.ImageDigest)
	}
	return &Instance{
		ID: NewID("inst"), AttemptID: l.AttemptID, LaunchID: l.ID, LaunchKey: l.LaunchKey, Backend: l.Backend,
		JobUID: jobUID, PodUID: podUID, ImageDigest: image, LaunchEpoch: l.LaunchEpoch, Phase: PhasePending,
		Current: !hasCurrent, RegisteredAt: now,
	}, nil
}

type Verdict string

const (
	VerdictCertified            Verdict = "certified"
	VerdictRepairable           Verdict = "repairable"
	VerdictInvalid              Verdict = "invalid"
	VerdictInfrastructureFailed Verdict = "infrastructure_failed"
	VerdictCanceled             Verdict = "canceled"
)

// Stage is an accepted immutable result of an attempt. ResultManifest holds
// the exact bytes the trusted observer submitted, which hash to
// ResultDigest; it is nil for stages accepted before the original bytes
// were retained (migration 00002), and such a stage must never be presented
// as byte-verified.
type Stage struct {
	ID               string
	AttemptID        string
	InstanceID       string
	OperationID      string
	PhaseOrdinal     uint64
	ProfileID        string
	Verdict          Verdict
	FailureCode      string
	ResultDigest     Digest
	ResultManifest   []byte
	ObserverIdentity string
	CommandID        string
	RequestDigest    Digest
	AcceptedAt       time.Time
	// The epochs the stage was accepted under and the finalized artifacts
	// it binds (every handle its manifest names, verified by Control).
	ExecutionEpoch uint64
	RecoveryEpoch  uint64
	Artifacts      []StageArtifact
}

// AcceptStage validates result acceptance: the instance must be the current
// physical owner of an attempt that is running under the current epoch and
// still inside its deadline; the observer's epoch, when stated, must be the
// operation's current one; the profile must match; exactly one stage per
// attempt phase is accepted. The stage records the epochs it was accepted
// under.
func AcceptStage(op *Operation, at *Attempt, inst *Instance, cmd CommandIdentity, profileID string, verdict Verdict, failureCode string, resultDigest Digest, manifest []byte, observer string, observerEpoch *uint64, now time.Time) (*Stage, error) {
	if !inst.Current || inst.AttemptID != at.ID {
		return nil, fmt.Errorf("%w: instance %s is not the current owner of attempt %s", ErrStaleExecution, inst.ID, at.ID)
	}
	if at.ExecutionEpoch != op.ExecutionEpoch {
		return nil, fmt.Errorf("%w: attempt epoch %d, operation epoch %d", ErrStaleExecution, at.ExecutionEpoch, op.ExecutionEpoch)
	}
	if observerEpoch != nil && *observerEpoch != op.ExecutionEpoch {
		return nil, fmt.Errorf("%w: the result was observed under execution epoch %d, operation %s is at %d", ErrStaleExecution, *observerEpoch, op.ID, op.ExecutionEpoch)
	}
	if op.FencedForNewDispatch() && op.Control == ControlNone {
		// A terminal or reconciling operation accepts no further result; a
		// cancel fence alone still lets the observer's verdict of the
		// already running instance be recorded.
		return nil, fmt.Errorf("%w: operation %s is %s", ErrStaleExecution, op.ID, op.Lifecycle)
	}
	if at.State != AttemptRunning && at.State != AttemptLaunchPrepared {
		return nil, fmt.Errorf("%w: attempt %s is %s", ErrStaleExecution, at.ID, at.State)
	}
	if profileID != at.ProfileID {
		return nil, fmt.Errorf("%w: result profile %q differs from attempt profile %q", ErrStaleExecution, profileID, at.ProfileID)
	}
	if now.After(at.Deadline) {
		return nil, fmt.Errorf("%w: attempt %s deadline passed", ErrStaleExecution, at.ID)
	}
	if verdict == VerdictCertified && failureCode != "" {
		return nil, fmt.Errorf("%w: certified verdict carries failure code", ErrInvalid)
	}
	if (verdict == VerdictInvalid || verdict == VerdictRepairable || verdict == VerdictInfrastructureFailed) && failureCode == "" {
		return nil, fmt.Errorf("%w: verdict %s requires a failure code", ErrInvalid, verdict)
	}
	return &Stage{
		ID: NewID("stg"), AttemptID: at.ID, InstanceID: inst.ID, OperationID: op.ID, PhaseOrdinal: 1,
		ProfileID: profileID, Verdict: verdict, FailureCode: failureCode, ResultDigest: resultDigest,
		ResultManifest: manifest, ObserverIdentity: observer, CommandID: cmd.CommandID, RequestDigest: cmd.RequestDigest,
		AcceptedAt: now, ExecutionEpoch: op.ExecutionEpoch, RecoveryEpoch: op.RecoveryEpoch,
	}, nil
}

// CanSettleCleanup reports whether a close request on an already closed
// attempt is later cleanup evidence: the same outcome, previously unknown
// cleanup, now a definite cleanup state. Only such evidence may move a
// reconciling operation on; any other repeated close is the original
// (same outcome) or a conflict (different outcome).
func (a *Attempt) CanSettleCleanup(outcome AttemptOutcome, cleanup CleanupState) bool {
	return a.State == AttemptClosed && a.Outcome == outcome && a.Cleanup == CleanupUnknown && cleanup != CleanupUnknown
}

// SettleClose derives the operation lifecycle from the attempt close for
// single-attempt profiles. An unknown outcome or unknown cleanup means the
// senders are not known to have stopped: the operation stays reconciling
// and a cancel fence stays pending rather than applied. Otherwise a cancel
// fence dominates, a certified accepted stage with complete cleanup
// succeeds, and anything else fails. Applied again with definite cleanup
// evidence (CanSettleCleanup), it moves the reconciling operation to its
// final lifecycle and clears the uncertainty code.
//
// For a multi-step profile (multiStep) a definite close settles the attempt
// only: the operation records the cleanup and phase and keeps running
// (or returns from reconciling to running when the evidence settles),
// because the Workflow states the business outcome through
// SettleOperation; an applied cancel still ends it, and an unknown
// outcome or cleanup still keeps it reconciling.
func SettleClose(op *Operation, at *Attempt, stage *Stage, outcome AttemptOutcome, cleanup CleanupState, failureCode string, multiStep bool) {
	at.State = AttemptClosed
	at.Outcome = outcome
	at.Cleanup = cleanup
	if failureCode != "" {
		at.FailureCode = failureCode
	}
	op.Cleanup = cleanup
	op.Phase = "closed"
	cancelRequested := op.Control == ControlCancelPending || op.Control == ControlCancelApplied || outcome == OutcomeCanceled
	switch {
	case outcome == OutcomeUnknown || cleanup == CleanupUnknown:
		op.Lifecycle = LifecycleReconciling
		op.FailureCode = firstNonEmpty(failureCode, at.FailureCode, "EFFECT_UNCERTAIN")
		if cancelRequested && op.Control != ControlCancelApplied {
			op.Control = ControlCancelPending
		}
	case multiStep && !cancelRequested:
		op.Lifecycle = LifecycleRunning
		op.Phase = "attempt_closed"
		op.FailureCode = ""
	case cancelRequested:
		op.Control = ControlCancelApplied
		op.Lifecycle = LifecycleCanceled
		op.FailureCode = ""
	case outcome == OutcomeCompleted && stage != nil && stage.Verdict == VerdictCertified && cleanup == CleanupComplete:
		op.Lifecycle = LifecycleSucceeded
		op.FailureCode = ""
	default:
		op.Lifecycle = LifecycleFailed
		op.FailureCode = firstNonEmpty(failureCode, at.FailureCode, string(outcome))
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
