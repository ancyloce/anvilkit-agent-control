// Package application composes Control's domain changes into transactions
// (the "one application transaction coordinator" of DD-02 §1) and owns the
// consumer-side ports the adapters implement. It knows nothing about gRPC,
// Fx or pgx types.
package application

import (
	"context"
	"errors"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// ErrDuplicateKey is returned by the store when an insert hits a uniqueness
// constraint; the application then reloads and compares digests.
var ErrDuplicateKey = errors.New("duplicate key")

// Store runs one function inside one database transaction. Every mutation
// of Control state goes through Tx so projection, events and records commit
// together; no network, storage, Temporal or Kubernetes I/O happens inside.
type Store interface {
	Tx(ctx context.Context, fn func(Repo) error) error
	// Read runs fn on a non-transactional connection for scoped reads.
	Read(ctx context.Context, fn func(Repo) error) error
}

// Repo is the transactional record port. Lock* methods take row locks in
// Control's rank order: operation (2) before attempt/instance (3) before
// command/result (6) before events (7).
type Repo interface {
	InsertOperation(ctx context.Context, op *domain.Operation) error
	GetOperationByCommand(ctx context.Context, tenantID, commandID string) (*domain.Operation, error)
	GetOperationScoped(ctx context.Context, operationID, tenantID string) (*domain.Operation, error)
	LockOperation(ctx context.Context, operationID string) (*domain.Operation, error)
	UpdateOperation(ctx context.Context, op *domain.Operation) error
	InsertEvent(ctx context.Context, ev domain.Event) error
	ListEvents(ctx context.Context, operationID string, afterSeq uint64, limit int) ([]domain.Event, error)
	ListIntakePending(ctx context.Context, olderThan time.Time, limit int) ([]*domain.Operation, error)
	ListRelayPending(ctx context.Context, limit int) ([]*domain.Operation, error)

	GetCommand(ctx context.Context, tenantID, commandID string) (*domain.Command, error)
	InsertCommand(ctx context.Context, c *domain.Command) error
	SettlePendingCommands(ctx context.Context, operationID string, kind domain.CommandKind, outcome domain.CommandOutcome, rev domain.Revision, now time.Time) error

	GetAttemptByCommand(ctx context.Context, operationID, commandID string) (*domain.Attempt, error)
	GetAttempt(ctx context.Context, attemptID string) (*domain.Attempt, error)
	LockAttempt(ctx context.Context, attemptID string) (*domain.Attempt, error)
	CountAttempts(ctx context.Context, operationID, stepID string, visit uint64) (uint64, error)
	HasOpenAttempt(ctx context.Context, operationID string) (bool, error)
	InsertAttempt(ctx context.Context, at *domain.Attempt) error
	UpdateAttempt(ctx context.Context, at *domain.Attempt) error

	GetLaunchByCommand(ctx context.Context, attemptID, commandID string) (*domain.Launch, error)
	GetLaunchByKey(ctx context.Context, backend, launchKey string) (*domain.Launch, error)
	LockLaunch(ctx context.Context, launchID string) (*domain.Launch, error)
	InsertLaunch(ctx context.Context, l *domain.Launch) error
	UpdateLaunchInventory(ctx context.Context, l *domain.Launch) error

	GetInstanceByPod(ctx context.Context, backend, podUID string) (*domain.Instance, error)
	LockInstance(ctx context.Context, instanceID string) (*domain.Instance, error)
	HasCurrentInstance(ctx context.Context, attemptID string) (bool, error)
	InsertInstance(ctx context.Context, inst *domain.Instance) error
	UpdateInstanceObservation(ctx context.Context, inst *domain.Instance) error

	GetStageByAttempt(ctx context.Context, attemptID string) (*domain.Stage, error)
	InsertStage(ctx context.Context, st *domain.Stage) error
	InsertStageArtifact(ctx context.Context, a *domain.StageArtifact) error
	ListStageArtifacts(ctx context.Context, stageID string) ([]domain.StageArtifact, error)

	// Artifact transfers (rank 6): scoped uploads and their verified objects.
	GetTransferByCommand(ctx context.Context, tenantID, commandID string) (*domain.Transfer, error)
	GetTransfer(ctx context.Context, transferID string) (*domain.Transfer, error)
	GetTransferByHandle(ctx context.Context, handle string) (*domain.Transfer, error)
	LockTransfer(ctx context.Context, transferID string) (*domain.Transfer, error)
	InsertTransfer(ctx context.Context, t *domain.Transfer) error
	UpdateTransfer(ctx context.Context, t *domain.Transfer) error

	// Budgets (rank 1): pools and the operation's allocations.
	ListCurrentPools(ctx context.Context, currency string, tenantID, actorID string, at time.Time) ([]*domain.BudgetPool, error)
	LockPool(ctx context.Context, poolID string) (*domain.BudgetPool, error)
	UpdatePool(ctx context.Context, p *domain.BudgetPool) error
	LockAllocations(ctx context.Context, operationID string) ([]*domain.Allocation, error)
	GetAllocationByPool(ctx context.Context, poolID, operationID string) (*domain.Allocation, error)
	InsertAllocation(ctx context.Context, a *domain.Allocation) error
	UpdateAllocation(ctx context.Context, a *domain.Allocation) error

	// Dispatches (rank 4) and their ledger (rank 6).
	GetDispatchByCall(ctx context.Context, tenantID, owner, callID string) (*domain.Dispatch, error)
	FindDispatchByOwnerCall(ctx context.Context, owner, callID string) (*domain.Dispatch, error)
	GetDispatch(ctx context.Context, dispatchID string) (*domain.Dispatch, error)
	LockDispatch(ctx context.Context, dispatchID string) (*domain.Dispatch, error)
	InsertDispatch(ctx context.Context, d *domain.Dispatch) error
	UpdateDispatch(ctx context.Context, d *domain.Dispatch) error
	HasOverspend(ctx context.Context, operationID string) (bool, error)
	HasUnknownDispatch(ctx context.Context, operationID string) (bool, error)
	GetUsageObservation(ctx context.Context, dispatchID, source string, sequence uint64) (*domain.UsageObservation, error)
	InsertUsageObservation(ctx context.Context, o *domain.UsageObservation) error
	// MaxUsage is the running maximum of the explicitly reported counters;
	// HasReportedUsage says whether any report carried counters at all.
	MaxUsage(ctx context.Context, dispatchID string) (domain.Usage, error)
	HasReportedUsage(ctx context.Context, dispatchID string) (bool, error)
	// ChargedCost sums the actual and correction entries of a dispatch and
	// names its actual entry (the target of later corrections).
	ChargedCost(ctx context.Context, dispatchID string) (charged int64, actualEntryID string, err error)
	InsertCostEntry(ctx context.Context, e *domain.CostEntry) error

	// Grant policy projection (read; MCP owns the grant, P18 registers it).
	GetGrantPolicy(ctx context.Context, grantID string, revision uint64) (*domain.GrantPolicy, error)

	// Effects (rank 6): business-write obligations, their observations and
	// the evidence-bound dispositions of any obligation class.
	GetEffectByCommand(ctx context.Context, tenantID, commandID string) (*domain.Effect, error)
	GetEffectByOccurrence(ctx context.Context, operationID string, kind domain.EffectKind, occurrence uint64) (*domain.Effect, error)
	GetEffect(ctx context.Context, effectID string) (*domain.Effect, error)
	LockEffect(ctx context.Context, effectID string) (*domain.Effect, error)
	InsertEffect(ctx context.Context, e *domain.Effect) error
	UpdateEffect(ctx context.Context, e *domain.Effect) error
	HasUnresolvedEffect(ctx context.Context, operationID string) (bool, error)
	GetEffectObservation(ctx context.Context, effectID, source string, sequence uint64) (*domain.EffectObservation, error)
	InsertEffectObservation(ctx context.Context, o *domain.EffectObservation) error
	GetDisposition(ctx context.Context, class, obligationID string) (*domain.Disposition, error)
	GetDispositionByCommand(ctx context.Context, tenantID, commandID string) (*domain.Disposition, error)
	InsertDisposition(ctx context.Context, d *domain.Disposition) error

	// Recovery: the run row is locked before any operation (rank 1); the
	// admission closure of a scope is read inside every admitting
	// transaction.
	GetLaunch(ctx context.Context, launchID string) (*domain.Launch, error)
	GetRecoveryRunByCommand(ctx context.Context, scopeKey, commandID string) (*domain.RecoveryRun, error)
	GetRecoveryRun(ctx context.Context, runID string) (*domain.RecoveryRun, error)
	LockRecoveryRun(ctx context.Context, runID string) (*domain.RecoveryRun, error)
	MaxRecoveryEpoch(ctx context.Context) (uint64, error)
	InsertRecoveryRun(ctx context.Context, r *domain.RecoveryRun) error
	UpdateRecoveryRun(ctx context.Context, r *domain.RecoveryRun) error
	ListRecoveryPending(ctx context.Context, limit int) ([]*domain.RecoveryRun, error)
	ListActiveOperations(ctx context.Context, scopeKey string) ([]*domain.Operation, error)
	GetRecoveryProgress(ctx context.Context, runID, class string) (*domain.RecoveryProgress, error)
	UpsertRecoveryProgress(ctx context.Context, p *domain.RecoveryProgress) error
	ListRecoveryProgress(ctx context.Context, runID string) ([]*domain.RecoveryProgress, error)
	GetFinding(ctx context.Context, findingID string) (*domain.RecoveryFinding, error)
	LockFinding(ctx context.Context, findingID string) (*domain.RecoveryFinding, error)
	GetFindingByObligation(ctx context.Context, runID, class, obligationID string) (*domain.RecoveryFinding, error)
	InsertFinding(ctx context.Context, f *domain.RecoveryFinding) error
	UpdateFinding(ctx context.Context, f *domain.RecoveryFinding) error
	// ListFindings pages in (class, obligation_id) order after the given key.
	ListFindings(ctx context.Context, runID string, status domain.FindingStatus, afterClass, afterObligationID string, limit int) ([]*domain.RecoveryFinding, error)
	CountUnsettledFindings(ctx context.Context, runID string) (uint64, error)
	IsAdmissionClosed(ctx context.Context, tenantID string) (bool, error)
	GetAdmissionClosure(ctx context.Context, scopeKey string) (*domain.AdmissionClosure, error)
	UpsertAdmissionClosure(ctx context.Context, c *domain.AdmissionClosure) error
	ReopenAdmission(ctx context.Context, scopeKey, runID string, at time.Time) error
}

// Inventory is the durable external-obligation port (DD-02 §5): the
// qualified independent-failure-domain backend in production, the
// DEVELOPMENT_ONLY filesystem adapter in development.
//
// Put creates an immutable object outside database locks: the same key and
// body is idempotent and returns the existing version; a different body for
// the same key is domain.ErrIdempotencyConflict; any other failure leaves
// the write uncertain and the caller must not assume absence (the object
// may have been published before the acknowledgement was lost).
//
// Get reads an object by its original key; a missing key is
// domain.ErrNotFound. List enumerates the objects under a prefix in key
// order, one page per call, resuming from the cursor of the previous page
// ("" starts at the beginning). A page that cannot be read is an error and
// never an empty page: an interrupted or unavailable listing establishes
// nothing about the inventory, so the caller keeps its cursor and retries.
type Inventory interface {
	Put(ctx context.Context, key string, body []byte) (version string, err error)
	Get(ctx context.Context, key string) (body []byte, version string, err error)
	List(ctx context.Context, prefix, cursor string, limit int) (InventoryPage, error)
}

// InventoryObject is one listed object: its key, immutable version and the
// backend's modification time (a bound for the rollback window; the
// obligation's own timestamp is in its body).
type InventoryObject struct {
	Key        string
	Version    string
	ModifiedAt time.Time
	Size       int64
}

// InventoryPage is one page of a listing. Complete is true when the page
// is the last one; otherwise NextCursor resumes the listing after it.
type InventoryPage struct {
	Objects    []InventoryObject
	NextCursor string
	Complete   bool
}

// ArtifactStore is the scoped object store of the artifact classes (DD-02
// §6): a separate backend, bucket and credential from the obligation
// inventory. UploadCapability issues the time-bounded authorization for
// one upload of exactly the declared bytes under the key (no request is
// made); Read returns the bytes and facts of the exact object version,
// domain.ErrNotFound when the key or version does not exist, and
// domain.ErrInvalid when the object exceeds the limit. Both are called
// outside every database lock. Nothing here deletes: accepted objects and
// their audit stay (garbage collection of proven-unreferenced objects is a
// later reviewed retention task).
type ArtifactStore interface {
	UploadCapability(ctx context.Context, key, mediaType string, size int64, expiresAt time.Time) (UploadCapability, error)
	Read(ctx context.Context, key, version string, limit int64) ([]byte, domain.ObjectFacts, error)
}

// UploadCapability is one upload authorization: method, URL and the
// headers the request must carry as signed, valid until ExpiresAt.
type UploadCapability struct {
	URL       string
	Method    string
	Headers   map[string]string
	ExpiresAt time.Time
}

// WorkflowRelay starts and cancels business workflows by operation
// identity. Both calls are idempotent per identity; Control never chooses
// the next business action.
type WorkflowRelay interface {
	Start(ctx context.Context, operationID, tenantID string) (runID string, err error)
	Cancel(ctx context.Context, operationID string) error
	// StartRecovery starts the reconciliation workflow of a recovery run;
	// idempotent by run identity.
	StartRecovery(ctx context.Context, runID string) (temporalRunID string, err error)
}

// ManifestValidator exposes the jobs contract (contracts/jobs): it
// validates a result manifest against the schema and answers the reviewed
// job profile a result claims to come from (domain.ErrNotFound for an id
// no reviewed profile carries).
type ManifestValidator interface {
	ValidateResultManifest(manifest []byte) error
	JobProfile(profileID string) (domain.JobProfile, error)
}

// PriceBook is the source of immutable pricing observations (DD-02 §3). A
// route without a price effective at admission time denies the paid route;
// the production source is an ENV-06 input, the development source a
// reviewed fixture that is explicitly DEVELOPMENT_ONLY.
type PriceBook interface {
	// Price returns the observation covering the route at the instant.
	Price(kind domain.DispatchKind, route string, at time.Time) (*domain.Price, bool)
	// PriceByRevision returns the exact observation a dispatch was metered
	// under, so later usage is priced under the same revision.
	PriceByRevision(revision string) (*domain.Price, bool)
}

// Authority is Control's guarded read of current commercial authority for
// a model route (DD-02 §3): a decision with an absolute freshness bound.
// It is consulted outside database locks; the decision is rechecked for
// freshness inside the consuming transaction and never refreshed by a hit.
// CheckOperator decides whether the actor may record an operator
// disposition for the tenant (platform-owned action; Casbin in production,
// the DEVELOPMENT_ONLY fixture in development).
type Authority interface {
	CheckExecution(ctx context.Context, scope domain.Scope, routeID string) (domain.Decision, error)
	CheckOperator(ctx context.Context, scope domain.Scope, action string) (domain.Decision, error)
}

// OutcomeQuery answers the query of an obligation's original identity
// (DD-06 §2 query-by-identity, DD-02 §4 "query original ID"): what the
// upstream or the sender knows about the call or mutation. Known false
// with a nil error means the upstream has no record; an error means the
// query could not be made, which establishes nothing. The production
// adapters are Model Proxy (P11) and Pagix (ENV-07) declarations; the
// development adapter is an explicitly labeled controlled double.
type OutcomeQuery interface {
	QueryDispatch(ctx context.Context, d *domain.Dispatch) (DispatchOutcomeReport, error)
	QueryEffect(ctx context.Context, e *domain.Effect) (EffectOutcomeReport, error)
}

// DispatchOutcomeReport is what the original-identity query of a call
// returned; Source and Sequence deduplicate it as an observation.
type DispatchOutcomeReport struct {
	Known           bool
	Source          string
	Sequence        uint64
	Outcome         domain.DispatchOutcome
	Usage           *domain.Usage
	NativeReference string
	ObservedAt      time.Time
}

// EffectOutcomeReport is what the original-identity query of a guarded
// mutation returned.
type EffectOutcomeReport struct {
	Known           bool
	Source          string
	Sequence        uint64
	Outcome         domain.EffectOutcome
	ReceiptRef      string
	NativeReference string
	ObservedAt      time.Time
}

// DispositionEvidence verifies, outside database locks, the evidence an
// operator offers for a disposition: independently readable, hashing to
// the offered digest, naming the obligation and the decision, from a
// trusted issuer. Anything less is domain.ErrEvidenceInsufficient.
type DispositionEvidence interface {
	VerifyDisposition(ctx context.Context, class, obligationID string, decision domain.DispositionDecision, evidenceRef string, evidenceDigest domain.Digest) error
}

// NotSentEvidence verifies, outside database locks, that the evidence a
// caller offers proves the original sender can no longer send the call
// (DD-02 §4): independently checkable, bound to the dispatch, from a
// trusted issuer. Anything less is domain.ErrEvidenceInsufficient; the
// caller's own assertion, a missing log, a timeout or an expired lease
// never qualify.
type NotSentEvidence interface {
	VerifyNotSent(ctx context.Context, d *domain.Dispatch, evidenceRef string, evidenceDigest domain.Digest) error
}
