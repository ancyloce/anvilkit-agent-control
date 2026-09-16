package domain

import (
	"fmt"
	"strings"
	"time"
)

// ArtifactClass is one of the eight artifact classes of the contract
// (contracts/proto/anvilkit/control/v1/artifact.proto, openapi/agent.yaml);
// no other class exists on this surface.
type ArtifactClass string

const (
	ArtifactPrompt   ArtifactClass = "prompt"
	ArtifactBrief    ArtifactClass = "brief"
	ArtifactSource   ArtifactClass = "source"
	ArtifactStage    ArtifactClass = "stage"
	ArtifactResult   ArtifactClass = "result"
	ArtifactEvidence ArtifactClass = "evidence"
	ArtifactAnswer   ArtifactClass = "answer"
	ArtifactArgument ArtifactClass = "argument"
)

// ArtifactClasses lists the classes in contract order.
var ArtifactClasses = []ArtifactClass{ArtifactPrompt, ArtifactBrief, ArtifactSource, ArtifactStage, ArtifactResult, ArtifactEvidence, ArtifactAnswer, ArtifactArgument}

func ParseArtifactClass(s string) (ArtifactClass, error) {
	for _, c := range ArtifactClasses {
		if string(c) == s {
			return c, nil
		}
	}
	return "", fmt.Errorf("%w: artifact class %q", ErrInvalid, s)
}

// TransferState is the life of one scoped transfer:
//
//	begun ──finalize (bytes verified)──▶ finalized
//	  │
//	  ├──verification failed / scope stale──▶ rejected
//	  └──deadline passed──────────────────▶ expired
//
// Only a finalized transfer holds a verified object version; rejected and
// expired are terminal, and a new transfer is needed for another upload.
type TransferState string

const (
	TransferBegun     TransferState = "begun"
	TransferFinalized TransferState = "finalized"
	TransferRejected  TransferState = "rejected"
	TransferExpired   TransferState = "expired"
)

// Reason codes recorded on a rejected or expired transfer.
const (
	ReasonDeadlineExceeded = "DEADLINE_EXCEEDED"
	ReasonSizeMismatch     = "SIZE_MISMATCH"
	ReasonDigestMismatch   = "DIGEST_MISMATCH"
	ReasonPathEscape       = "PATH_ESCAPE"
	ReasonArchiveInvalid   = "ARCHIVE_INVALID"
	ReasonStaleExecution   = "STALE_EXECUTION"
)

// Transfer is one scoped artifact transfer (DD-02 §6, contracts.md §2
// artifact_transfers): the scope it was begun under (tenant, actor, and
// when bound the operation and attempt with their epochs), the exact bytes
// expected, the opaque handle candidates receive, the object key only
// Control knows, and, once finalized, the verified facts of the object.
type Transfer struct {
	ID                    string
	TenantID              string
	ActorID               string
	OperationID           string
	AttemptID             string
	Class                 ArtifactClass
	MediaType             string
	ExpectedDigest        Digest
	ExpectedSize          int64
	Handle                string
	State                 TransferState
	ObjectKey             string
	ObjectVersion         string
	ActualSize            int64
	ActualDigest          Digest
	ReasonCode            string
	CommandID             string
	RequestDigest         Digest
	FinalizeCommandID     string
	FinalizeRequestDigest Digest
	ExecutionEpoch        uint64
	RecoveryEpoch         uint64
	Deadline              time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
	FinalizedAt           *time.Time
}

// TransferRequest is what a caller submits to begin a transfer.
type TransferRequest struct {
	Class          ArtifactClass
	MediaType      string
	ExpectedDigest Digest
	ExpectedSize   int64
	OperationID    string
	AttemptID      string
	Deadline       time.Time
}

// TransferLimits are the reviewed bounds of the artifact service.
type TransferLimits struct {
	MaxObjectBytes int64
	MaxWindow      time.Duration
}

// TransferContext is the current state a begin runs against, loaded under
// the lock ranks by the application: the operation (2) and attempt (3)
// when the request binds them, and the clock read after the locks.
type TransferContext struct {
	Now       time.Time
	Operation *Operation
	Attempt   *Attempt
	Limits    TransferLimits
}

// NewTransfer validates a begin against its scope and returns the record
// in the begun state. A bound transfer takes the epochs of its operation
// and never outlives the operation's or attempt's deadline; a fenced
// operation or a closed attempt admits no transfer.
func NewTransfer(cmd CommandIdentity, scope Scope, req TransferRequest, c TransferContext) (*Transfer, error) {
	if _, err := ParseArtifactClass(string(req.Class)); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.MediaType) == "" || len(req.MediaType) > 128 {
		return nil, fmt.Errorf("%w: media type is required", ErrInvalid)
	}
	if _, err := ParseDigest(string(req.ExpectedDigest)); err != nil {
		return nil, err
	}
	if req.ExpectedSize < 0 {
		return nil, fmt.Errorf("%w: expected size must not be negative", ErrInvalid)
	}
	if c.Limits.MaxObjectBytes > 0 && req.ExpectedSize > c.Limits.MaxObjectBytes {
		return nil, fmt.Errorf("%w: expected size %d exceeds the artifact bound %d", ErrInvalid, req.ExpectedSize, c.Limits.MaxObjectBytes)
	}
	deadline := req.Deadline.UTC().Truncate(time.Microsecond)
	if !deadline.After(c.Now) {
		return nil, fmt.Errorf("%w: transfer deadline %s already passed", ErrStaleExecution, deadline.Format(time.RFC3339))
	}
	if c.Limits.MaxWindow > 0 && deadline.After(c.Now.Add(c.Limits.MaxWindow)) {
		return nil, fmt.Errorf("%w: transfer deadline %s exceeds the window bound %s", ErrInvalid, deadline.Format(time.RFC3339), c.Limits.MaxWindow)
	}
	t := &Transfer{
		ID: NewID("xfer"), TenantID: scope.TenantID, ActorID: scope.ActorID, OperationID: req.OperationID, AttemptID: req.AttemptID, Class: req.Class,
		MediaType: req.MediaType, ExpectedDigest: req.ExpectedDigest, ExpectedSize: req.ExpectedSize, Handle: NewID("hdl"), State: TransferBegun,
		CommandID: cmd.CommandID, RequestDigest: cmd.RequestDigest, ExecutionEpoch: 1, RecoveryEpoch: 1, Deadline: deadline, CreatedAt: c.Now, UpdatedAt: c.Now,
	}
	if req.AttemptID != "" && req.OperationID == "" {
		return nil, fmt.Errorf("%w: an attempt-bound transfer names its operation", ErrInvalid)
	}
	if req.OperationID != "" {
		op := c.Operation
		if op == nil || op.ID != req.OperationID || op.TenantID != scope.TenantID {
			return nil, fmt.Errorf("%w: operation %s", ErrNotFound, req.OperationID)
		}
		if op.FencedForNewDispatch() {
			return nil, fmt.Errorf("%w: operation %s is fenced (%s/%s)", ErrStaleExecution, op.ID, op.Lifecycle, op.Control)
		}
		if deadline.After(op.Deadline) {
			deadline = op.Deadline
		}
		t.ExecutionEpoch, t.RecoveryEpoch = op.ExecutionEpoch, op.RecoveryEpoch
		if req.AttemptID != "" {
			at := c.Attempt
			if at == nil || at.ID != req.AttemptID || at.OperationID != op.ID {
				return nil, fmt.Errorf("%w: attempt %s of operation %s", ErrNotFound, req.AttemptID, op.ID)
			}
			if at.ExecutionEpoch != op.ExecutionEpoch {
				return nil, fmt.Errorf("%w: attempt epoch %d, operation epoch %d", ErrStaleExecution, at.ExecutionEpoch, op.ExecutionEpoch)
			}
			if at.State == AttemptClosed || at.State == AttemptResultAccepted {
				return nil, fmt.Errorf("%w: attempt %s is %s", ErrStaleExecution, at.ID, at.State)
			}
			if deadline.After(at.Deadline) {
				deadline = at.Deadline
			}
		}
		if !deadline.After(c.Now) {
			return nil, fmt.Errorf("%w: the bound deadline %s already passed", ErrStaleExecution, deadline.Format(time.RFC3339))
		}
		t.Deadline = deadline
	}
	t.ObjectKey = t.TenantID + "/" + string(t.Class) + "/" + t.ID
	return t, nil
}

// Binds verifies that a begin reentering the persisted transfer is the
// same request; a changed field under the same command is an idempotency
// conflict.
func (t *Transfer) Binds(scope Scope, req TransferRequest, digest Digest) error {
	conflict := func(field string, recorded, offered any) error {
		return fmt.Errorf("%w: transfer %s was begun with %s %v, the request names %v", ErrIdempotencyConflict, t.ID, field, recorded, offered)
	}
	switch {
	case t.TenantID != scope.TenantID:
		return fmt.Errorf("%w: transfer %s", ErrNotFound, t.ID)
	case t.RequestDigest != digest:
		return conflict("digest", t.RequestDigest, digest)
	case t.Class != req.Class:
		return conflict("class", t.Class, req.Class)
	case t.MediaType != req.MediaType:
		return conflict("media type", t.MediaType, req.MediaType)
	case t.ExpectedDigest != req.ExpectedDigest:
		return conflict("expected digest", t.ExpectedDigest, req.ExpectedDigest)
	case t.ExpectedSize != req.ExpectedSize:
		return conflict("expected size", t.ExpectedSize, req.ExpectedSize)
	case t.OperationID != req.OperationID:
		return conflict("operation", t.OperationID, req.OperationID)
	case t.AttemptID != req.AttemptID:
		return conflict("attempt", t.AttemptID, req.AttemptID)
	}
	return nil
}

// InScope reports whether the transfer belongs to the tenant.
func (t *Transfer) InScope(tenantID string) bool { return t.TenantID == tenantID }

// ScopeContext is the current state of a transfer's binding, loaded by
// the application (read for a resolve, locked for a finalize).
type ScopeContext struct {
	Now       time.Time
	Operation *Operation // nil for an unbound transfer
	Attempt   *Attempt   // nil when the transfer names no attempt
	Instance  *Instance  // the resolving physical instance, for Resolvable
}

// current verifies that a bound transfer's scope is still current: the
// operation is not fenced and its epochs are the ones the transfer was
// begun under, the attempt (when bound) is still executing under them.
func (t *Transfer) current(c ScopeContext) error {
	if t.OperationID == "" {
		return nil
	}
	op := c.Operation
	if op == nil || op.ID != t.OperationID {
		return fmt.Errorf("%w: operation %s of transfer %s", ErrNotFound, t.OperationID, t.ID)
	}
	if op.FencedForNewDispatch() {
		return fmt.Errorf("%w: operation %s is fenced (%s/%s)", ErrStaleExecution, op.ID, op.Lifecycle, op.Control)
	}
	if op.ExecutionEpoch != t.ExecutionEpoch || op.RecoveryEpoch != t.RecoveryEpoch {
		return fmt.Errorf("%w: transfer %s was begun under epochs %d/%d, operation %s is at %d/%d", ErrStaleExecution, t.ID, t.ExecutionEpoch, t.RecoveryEpoch, op.ID, op.ExecutionEpoch, op.RecoveryEpoch)
	}
	if t.AttemptID != "" {
		at := c.Attempt
		if at == nil || at.ID != t.AttemptID || at.OperationID != op.ID {
			return fmt.Errorf("%w: attempt %s of transfer %s", ErrNotFound, t.AttemptID, t.ID)
		}
		if at.ExecutionEpoch != op.ExecutionEpoch {
			return fmt.Errorf("%w: attempt epoch %d, operation epoch %d", ErrStaleExecution, at.ExecutionEpoch, op.ExecutionEpoch)
		}
		if at.State == AttemptClosed || at.State == AttemptResultAccepted {
			return fmt.Errorf("%w: attempt %s is %s", ErrStaleExecution, at.ID, at.State)
		}
	}
	return nil
}

// Reissuable decides whether the upload capability of the transfer may be
// issued (again) to the trusted caller that begins or reenters it: the
// transfer is still begun and inside its deadline, and its scope is
// current as of the decision (the operation not fenced, the epochs the
// ones it was begun under, the attempt still executing). A cancel, a
// moved epoch, a closed attempt or a passed deadline leaves the original
// transfer state without fresh upload authority. Unlike Finalizable it
// records nothing.
func (t *Transfer) Reissuable(c ScopeContext) error {
	if t.State != TransferBegun {
		return fmt.Errorf("%w: transfer %s is %s", ErrStaleExecution, t.ID, t.State)
	}
	if !c.Now.Before(t.Deadline) {
		return fmt.Errorf("%w: transfer %s deadline %s passed", ErrStaleExecution, t.ID, t.Deadline.UTC().Format(time.RFC3339))
	}
	return t.current(c)
}

// Resolvable decides whether a physical instance may receive the upload
// capability of the transfer: the transfer is begun and inside its
// deadline, bound to an attempt, and the instance is the current physical
// owner of that attempt under the current epochs. A candidate's handle is
// worth nothing outside that binding.
func (t *Transfer) Resolvable(c ScopeContext) error {
	if t.State != TransferBegun {
		return fmt.Errorf("%w: transfer %s is %s", ErrStaleExecution, t.ID, t.State)
	}
	if !c.Now.Before(t.Deadline) {
		return fmt.Errorf("%w: transfer %s deadline %s passed", ErrStaleExecution, t.ID, t.Deadline.UTC().Format(time.RFC3339))
	}
	if t.AttemptID == "" {
		return fmt.Errorf("%w: transfer %s is not bound to an attempt; no instance may resolve it", ErrForbidden, t.ID)
	}
	inst := c.Instance
	if inst == nil || inst.AttemptID != t.AttemptID {
		return fmt.Errorf("%w: instance is not an instance of attempt %s", ErrNotFound, t.AttemptID)
	}
	if !inst.Current {
		return fmt.Errorf("%w: instance %s is not the current physical owner of attempt %s", ErrStaleExecution, inst.ID, t.AttemptID)
	}
	return t.current(c)
}

// ObjectFacts are what Control established by reading the object version
// itself: its version, size and digest.
type ObjectFacts struct {
	Version string
	Size    int64
	Digest  Digest
}

// Finalizable decides, under the transfer lock, whether a finalize may
// commit: the transfer is still begun, inside its deadline, and its scope
// is current. A transfer whose deadline passed becomes expired here; a
// transfer whose scope moved on becomes rejected.
func (t *Transfer) Finalizable(c ScopeContext) error {
	switch t.State {
	case TransferBegun:
	case TransferFinalized:
		return nil
	default:
		return fmt.Errorf("%w: transfer %s is %s (%s)", ErrStaleExecution, t.ID, t.State, t.ReasonCode)
	}
	if !c.Now.Before(t.Deadline) {
		t.State, t.ReasonCode, t.UpdatedAt = TransferExpired, ReasonDeadlineExceeded, c.Now
		return fmt.Errorf("%w: transfer %s deadline %s passed", ErrStaleExecution, t.ID, t.Deadline.UTC().Format(time.RFC3339))
	}
	if err := t.current(c); err != nil {
		t.State, t.ReasonCode, t.UpdatedAt = TransferRejected, ReasonStaleExecution, c.Now
		return err
	}
	return nil
}

// Finalize records the verified object. The facts must be what Control
// read from the named version: a size or digest other than the declared
// one rejects the transfer permanently (the object under the key stays as
// it is; nothing is deleted), and a rejected transfer is never finalized
// by a later upload.
func (t *Transfer) Finalize(cmd CommandIdentity, facts ObjectFacts, now time.Time) error {
	if t.State != TransferBegun {
		return fmt.Errorf("%w: transfer %s is %s", ErrStaleExecution, t.ID, t.State)
	}
	if facts.Version == "" {
		return fmt.Errorf("%w: the object version is required", ErrInvalid)
	}
	switch {
	case facts.Size != t.ExpectedSize:
		t.State, t.ReasonCode, t.UpdatedAt = TransferRejected, ReasonSizeMismatch, now
		return fmt.Errorf("%w: object version %s holds %d bytes, the transfer declared %d", ErrInvalid, facts.Version, facts.Size, t.ExpectedSize)
	case facts.Digest != t.ExpectedDigest:
		t.State, t.ReasonCode, t.UpdatedAt = TransferRejected, ReasonDigestMismatch, now
		return fmt.Errorf("%w: object version %s hashes to %s, the transfer declared %s", ErrInvalid, facts.Version, facts.Digest, t.ExpectedDigest)
	}
	t.State, t.ObjectVersion, t.ActualSize, t.ActualDigest = TransferFinalized, facts.Version, facts.Size, facts.Digest
	t.FinalizeCommandID, t.FinalizeRequestDigest, t.FinalizedAt, t.UpdatedAt = cmd.CommandID, cmd.RequestDigest, &now, now
	return nil
}

// Reject records a verification failure other than size or digest (an
// archive that escapes its root, an object beyond the bound).
func (t *Transfer) Reject(reason string, now time.Time) {
	if t.State == TransferBegun {
		t.State, t.ReasonCode, t.UpdatedAt = TransferRejected, reason, now
	}
}

// Reenters reports whether a finalize command is the one that finalized
// the transfer (same command and digest, same object version): the
// repeated call returns the original outcome. A finalized transfer named
// with another version, or a different command claiming the same version,
// is an idempotency conflict.
func (t *Transfer) Reenters(cmd CommandIdentity, objectVersion string) error {
	if t.State != TransferFinalized {
		return nil
	}
	if t.FinalizeCommandID == cmd.CommandID && t.FinalizeRequestDigest == cmd.RequestDigest && t.ObjectVersion == objectVersion {
		return nil
	}
	return fmt.Errorf("%w: transfer %s was finalized by command %s at object version %s", ErrIdempotencyConflict, t.ID, t.FinalizeCommandID, t.ObjectVersion)
}

// StageArtifact is one finalized artifact an accepted stage binds: the
// transfer, the class, digest and size the manifest declared and Control
// verified, and the exact object version.
type StageArtifact struct {
	StageID       string
	TransferID    string
	Handle        string
	Class         ArtifactClass
	Digest        Digest
	SizeBytes     int64
	ObjectVersion string
}

// ManifestOutput is one output of a result manifest as the trusted
// observer submitted it (contracts/jobs resultManifest.outputs).
type ManifestOutput struct {
	Class     string
	Digest    Digest
	SizeBytes int64
	Handle    string
}

// ResultManifest is what acceptance reads of a result manifest
// (contracts/jobs resultManifest): the provenance it claims and the
// outputs it names. The bytes themselves are validated against the jobs
// contract and retained; this is the checked view of them.
type ResultManifest struct {
	LaunchID    string
	AttemptID   string
	JobKind     string
	ProfileID   string
	Verdict     Verdict
	FailureCode string
	Outputs     []ManifestOutput
}

// FixedResult is the one fixed result a qualification fixture profile
// declares (contracts/jobs profile.expectedResult): the reviewed digest
// and byte size its trusted observer compares the report with and Control
// binds an embedded result output to. Both come from the reviewed profile;
// a size the profile does not carry is not verified by anything.
type FixedResult struct {
	Digest    Digest
	SizeBytes int64
}

// JobProfile is what acceptance needs of a reviewed job profile
// (contracts/jobs/profiles.json): its kind, whether it runs candidate
// code and, for a qualification fixture, its fixed result (nil for a
// profile that declares none).
type JobProfile struct {
	ID             string
	JobKind        string
	CandidateCode  bool
	ExpectedResult *FixedResult
}

// prescribesFixedResult reports whether the profile prescribes its fixed
// result for a verdict: only a qualification fixture profile (no candidate
// code, a reviewed fixed result) under a certified verdict does. A failed
// fixture run and every other profile prescribe no output on this
// surface; their outputs are each bound to finalized objects.
func (p JobProfile) prescribesFixedResult(verdict Verdict) bool {
	return p.ExpectedResult != nil && !p.CandidateCode && verdict == VerdictCertified
}

// isFixedResult reports whether an output is exactly the reviewed fixed
// result: class result, the reviewed digest and the reviewed byte size.
func (p JobProfile) isFixedResult(out ManifestOutput) bool {
	return p.ExpectedResult != nil && out.Class == string(ArtifactResult) && out.Digest == p.ExpectedResult.Digest && out.SizeBytes == p.ExpectedResult.SizeBytes
}

// Claims verifies the manifest's provenance against the acceptance
// request and the reviewed profile: the attempt, profile and verdict the
// observer submitted, the failure code (present exactly when the request
// carries one, and the same), and the job kind of the reviewed profile
// the attempt runs. A manifest that claims otherwise is refused before
// anything is locked.
func (m ResultManifest) Claims(attemptID, profileID string, verdict Verdict, failureCode string, profile JobProfile) error {
	switch {
	case m.AttemptID != attemptID || m.ProfileID != profileID || m.Verdict != verdict:
		return fmt.Errorf("%w: result manifest identity does not match the acceptance request", ErrInvalid)
	case m.FailureCode != failureCode:
		return fmt.Errorf("%w: result manifest failure code %q conflicts with the acceptance request's %q", ErrInvalid, m.FailureCode, failureCode)
	case profile.ID != profileID:
		return fmt.Errorf("%w: result manifest names profile %s, the reviewed profile is %s", ErrInvalid, profileID, profile.ID)
	case m.JobKind != profile.JobKind:
		return fmt.Errorf("%w: result manifest claims job kind %q, profile %s is a %q job", ErrInvalid, m.JobKind, profile.ID, profile.JobKind)
	}
	return nil
}

// Delivers verifies, from the whole outputs list, that the manifest
// carries what the reviewed profile prescribes for its verdict: a
// certified result of a qualification fixture profile names the fixed
// result (class result, the reviewed digest and byte size) exactly once.
// An empty outputs list or another output in the fixed result's place is
// refused here rather than passed over by the per-output binding; a
// contract-valid failure manifest without outputs, and a profile that
// prescribes nothing, pass. Every output present is still bound by the
// caller.
func (m ResultManifest) Delivers(profile JobProfile) error {
	if !profile.prescribesFixedResult(m.Verdict) {
		return nil
	}
	fixed := 0
	for _, out := range m.Outputs {
		if profile.isFixedResult(out) {
			fixed++
		}
	}
	switch fixed {
	case 1:
		return nil
	case 0:
		return fmt.Errorf("%w: a certified result of profile %s names its fixed result %s (%d bytes); the manifest names %d output(s), none of them", ErrInvalid, profile.ID, profile.ExpectedResult.Digest, profile.ExpectedResult.SizeBytes, len(m.Outputs))
	default:
		return fmt.Errorf("%w: the fixed result of profile %s is named %d times", ErrInvalid, profile.ID, fixed)
	}
}

// ProducedBy verifies that the manifest names the launch of the physical
// instance the result is accepted from: a manifest of another launch, or
// of no launch this instance came from, is not this instance's result.
func (m ResultManifest) ProducedBy(inst *Instance) error {
	if inst == nil || m.LaunchID != inst.LaunchID {
		return fmt.Errorf("%w: result manifest names launch %q, the accepting instance came from another launch", ErrStaleExecution, m.LaunchID)
	}
	return nil
}

// EmbeddedOutput decides whether an output without a handle, one backed
// by no finalized transfer, may be accepted at all: only the fixed result
// of a qualification fixture profile (P05's LocalCheck): a profile that
// runs no candidate code and declares the reviewed digest and byte size
// its trusted observer compares the report with, for an output of class
// result with exactly that digest and size, under a certified verdict.
// Every other output (a source, an evidence, a result of another digest
// or size, anything under a candidate-code profile) must name a finalized
// object of the result's scope; omitting or emptying the handle never
// bypasses that.
func (p JobProfile) EmbeddedOutput(out ManifestOutput, verdict Verdict) error {
	switch {
	case p.ExpectedResult == nil || p.CandidateCode:
		return fmt.Errorf("%w: %s output names no finalized object; profile %s embeds no output", ErrInvalid, out.Class, p.ID)
	case out.Class != string(ArtifactResult):
		return fmt.Errorf("%w: %s output names no finalized object; fixture profile %s embeds only its fixed result", ErrInvalid, out.Class, p.ID)
	case !p.isFixedResult(out):
		return fmt.Errorf("%w: embedded result %s (%d bytes) is not the reviewed fixed result %s (%d bytes) of profile %s", ErrInvalid, out.Digest, out.SizeBytes, p.ExpectedResult.Digest, p.ExpectedResult.SizeBytes, p.ID)
	case verdict != VerdictCertified:
		return fmt.Errorf("%w: the fixed result of profile %s is embedded only under a certified verdict, not %s", ErrInvalid, p.ID, verdict)
	}
	return nil
}

// outputClasses maps a result manifest output class to the artifact class
// its handle must be a finalized transfer of. Output classes without an
// artifact class on this surface (the build deliverables of P10 and the
// parser chunks of P15) cannot be bound here.
var outputClasses = map[string]ArtifactClass{"source": ArtifactSource, "evidence": ArtifactEvidence, "result": ArtifactResult}

// BindArtifact verifies that a manifest output names a finalized transfer
// of the result's own scope and exact content: same tenant and operation,
// the same attempt when the transfer is attempt-bound, the operation's
// current epochs, the output's class, digest and size equal to what
// Control verified. It returns the stage artifact to record.
func BindArtifact(out ManifestOutput, t *Transfer, op *Operation, at *Attempt) (StageArtifact, error) {
	class, ok := outputClasses[out.Class]
	if !ok {
		return StageArtifact{}, fmt.Errorf("%w: output class %q has no artifact class on this surface; its unit defines the binding", ErrInvalid, out.Class)
	}
	if t == nil || t.TenantID != at.TenantID || t.OperationID != op.ID || (t.AttemptID != "" && t.AttemptID != at.ID) {
		return StageArtifact{}, fmt.Errorf("%w: output %q names a transfer outside the result's scope", ErrStaleExecution, out.Handle)
	}
	if t.State != TransferFinalized {
		return StageArtifact{}, fmt.Errorf("%w: output %q names transfer %s, which is %s, not finalized", ErrStaleExecution, out.Handle, t.ID, t.State)
	}
	if t.ExecutionEpoch != op.ExecutionEpoch || t.RecoveryEpoch != op.RecoveryEpoch {
		return StageArtifact{}, fmt.Errorf("%w: transfer %s was finalized under epochs %d/%d, the operation is at %d/%d", ErrStaleExecution, t.ID, t.ExecutionEpoch, t.RecoveryEpoch, op.ExecutionEpoch, op.RecoveryEpoch)
	}
	if t.Class != class {
		return StageArtifact{}, fmt.Errorf("%w: output %q is of class %s, transfer %s holds a %s artifact", ErrInvalid, out.Handle, out.Class, t.ID, t.Class)
	}
	if t.ActualDigest != out.Digest || t.ActualSize != out.SizeBytes {
		return StageArtifact{}, fmt.Errorf("%w: output %q declares %s (%d bytes), transfer %s verified %s (%d bytes)", ErrInvalid, out.Handle, out.Digest, out.SizeBytes, t.ID, t.ActualDigest, t.ActualSize)
	}
	return StageArtifact{TransferID: t.ID, Handle: t.Handle, Class: t.Class, Digest: t.ActualDigest, SizeBytes: t.ActualSize, ObjectVersion: t.ObjectVersion}, nil
}
