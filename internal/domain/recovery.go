package domain

import (
	"fmt"
	"time"
)

// RecoveryPhase is the ordered progression of one recovery run (DD-02 §5,
// platform.md "recovery gates"): admission is closed and old identities are
// fenced under a new recovery epoch first; the rollback window is then
// enumerated completely; the obligations missing from the restored
// database are reconciled under their original identities; the scope
// reopens only when every finding is resolved, otherwise it stays
// restricted.
type RecoveryPhase string

const (
	RecoveryFenced      RecoveryPhase = "fenced"
	RecoveryEnumerating RecoveryPhase = "enumerating"
	RecoveryReconciling RecoveryPhase = "reconciling"
	RecoveryRestricted  RecoveryPhase = "restricted"
	RecoveryReopened    RecoveryPhase = "reopened"
)

// ScopeAll is the scope key of a recovery run over every tenant.
const ScopeAll = "*"

// RecoveryRun is one reconciliation of a rollback window for a scope.
// WindowStart and WindowEnd bound the obligations to enumerate; Skew is
// the clock uncertainty added on both sides, so an obligation recorded
// near an edge is never excluded by a clock difference.
type RecoveryRun struct {
	ID            string
	ScopeKey      string
	TenantID      string // "" for ScopeAll
	CommandID     string
	ActorID       string
	RequestDigest Digest
	RecoveryEpoch uint64
	Phase         RecoveryPhase
	WindowStart   time.Time
	WindowEnd     time.Time
	Skew          time.Duration
	Reason        string
	FencedCount   uint64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	ReopenedAt    *time.Time
}

// Covers reports whether an obligation recorded at t (or listed with that
// modification time) falls inside the widened window.
func (r *RecoveryRun) Covers(t time.Time) bool {
	return !t.Before(r.WindowStart.Add(-r.Skew)) && !t.After(r.WindowEnd.Add(r.Skew))
}

// InScope reports whether the tenant belongs to the run's scope.
func (r *RecoveryRun) InScope(tenantID string) bool {
	return r.ScopeKey == ScopeAll || r.TenantID == tenantID
}

// NewRecoveryRun validates the window and returns the run in its first
// phase; the epoch is assigned by the application from the persisted
// maximum.
func NewRecoveryRun(cmd CommandIdentity, scopeKey string, epoch uint64, windowStart, windowEnd time.Time, skew time.Duration, reason string, now time.Time) (*RecoveryRun, error) {
	if scopeKey == "" {
		return nil, fmt.Errorf("%w: recovery scope is required", ErrInvalid)
	}
	if !windowEnd.After(windowStart) {
		return nil, fmt.Errorf("%w: recovery window end %s is not after start %s", ErrInvalid, windowEnd.UTC().Format(time.RFC3339), windowStart.UTC().Format(time.RFC3339))
	}
	if skew < 0 {
		return nil, fmt.Errorf("%w: clock uncertainty must not be negative", ErrInvalid)
	}
	tenant := scopeKey
	if scopeKey == ScopeAll {
		tenant = ""
	}
	return &RecoveryRun{
		ID: NewID("rec"), ScopeKey: scopeKey, TenantID: tenant, CommandID: cmd.CommandID, ActorID: cmd.ActorID, RequestDigest: cmd.RequestDigest,
		RecoveryEpoch: epoch, Phase: RecoveryFenced, WindowStart: windowStart.UTC().Truncate(time.Microsecond), WindowEnd: windowEnd.UTC().Truncate(time.Microsecond),
		Skew: skew, Reason: reason, CreatedAt: now, UpdatedAt: now,
	}, nil
}

// RecoveryProgress is the resumable cursor of one obligation class within
// a run: a listing that failed keeps its cursor and is never complete.
type RecoveryProgress struct {
	RunID    string
	Class    string
	Cursor   string
	Complete bool
	Seen     uint64
}

// FindingStatus is the state of one enumerated obligation.
type FindingStatus string

const (
	// FindingPresent: the restored database holds the identity.
	FindingPresent FindingStatus = "present"
	// FindingMissing: the inventory holds an obligation the database lost.
	FindingMissing FindingStatus = "missing"
	// FindingRestored: the identity was recreated from the inventory under
	// the new recovery epoch; its original outcome is still unknown.
	FindingRestored FindingStatus = "restored"
	// FindingResolved: the original outcome was observed or queried.
	FindingResolved FindingStatus = "resolved"
	// FindingUnresolved: the outcome query established nothing definite.
	FindingUnresolved FindingStatus = "unresolved"
	// FindingDisposed: an evidence-bound operator disposition settled it.
	FindingDisposed FindingStatus = "disposed"
	// FindingOutsideScope: the obligation belongs to another tenant.
	FindingOutsideScope FindingStatus = "outside_scope"
)

// Settled reports whether the finding no longer keeps the scope restricted.
func (s FindingStatus) Settled() bool {
	return s == FindingPresent || s == FindingResolved || s == FindingDisposed || s == FindingOutsideScope
}

// RecoveryFinding is one obligation of the window and what the run knows
// about it.
type RecoveryFinding struct {
	ID               string
	RunID            string
	Class            string
	ObligationID     string
	TenantID         string
	InventoryKey     string
	InventoryVersion string
	RecordedAt       time.Time
	Status           FindingStatus
	Outcome          string
	EvidenceRef      string
	Detail           string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	// LaunchKey is filled on read for job-launch findings whose launch the
	// database holds; it is not persisted with the finding.
	LaunchKey string
}

// DispositionDecision is an evidence-bound operator decision about one
// unresolved obligation. None of them erases incurred cost, approval or a
// partial upstream outcome; none of them applies to more than one
// obligation.
type DispositionDecision string

const (
	// DisposeConfirmNotSent: a trusted not-sent attestation proves the
	// original sender never sent; the exposure is released.
	DisposeConfirmNotSent DispositionDecision = "confirm_not_sent"
	// DisposeResolveSucceeded / DisposeResolveFailed: a trusted outcome
	// attestation names the upstream result of a business write.
	DisposeResolveSucceeded DispositionDecision = "resolve_succeeded"
	DisposeResolveFailed    DispositionDecision = "resolve_failed"
	// DisposeRetainExposure: the operator accepts that the outcome cannot be
	// established; the obligation stays unknown with its exposure retained,
	// and only the finding is settled so the scope can reopen.
	DisposeRetainExposure DispositionDecision = "retain_exposure"
)

// Disposition is the recorded operator decision.
type Disposition struct {
	ID             string
	Class          string
	ObligationID   string
	TenantID       string
	RunID          string
	RecoveryEpoch  uint64
	CommandID      string
	ActorID        string
	RequestDigest  Digest
	Decision       DispositionDecision
	EvidenceRef    string
	EvidenceDigest Digest
	Reason         string
	DecidedAt      time.Time
}

// AdmissionClosure records that a recovery run closed new admission for a
// scope; ReopenedAt is set when the run reopened it.
type AdmissionClosure struct {
	ScopeKey   string
	RunID      string
	ClosedAt   time.Time
	ReopenedAt *time.Time
}
