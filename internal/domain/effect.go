package domain

import (
	"fmt"
	"time"
)

// EffectKind is the class of guarded external mutation (DD-06 §3): the
// business write itself and the release-side mutations that share its
// permit rules.
type EffectKind string

const (
	EffectBusinessWrite EffectKind = "business_write"
	EffectPublication   EffectKind = "publication"
	EffectActivation    EffectKind = "activation"
	EffectReview        EffectKind = "review"
)

// EffectState is the permit state machine of one guarded mutation:
//
//	prepared ──permit──▶ permitted ──observe──▶ succeeded | failed
//	   │                    │                        ▲
//	   │                    └──observe(unknown)──▶ unknown ──observe──┘
//	   │                    │                        │
//	   │                    └────── evidence ───────▶ confirmed_not_sent ◀┘
//	   └──recheck fails──▶ denied
//
// PERMITTED means the single business-write permit was consumed by the
// registered physical owner, not that the upstream mutation happened. It is
// issued once; retries, timeouts, lease expiry and restarts never recreate
// it, and an unknown outcome keeps the original identity until an outcome
// observation, a query of the original identity or an evidence-bound
// operator disposition resolves it.
type EffectState string

const (
	EffectPrepared         EffectState = "prepared"
	EffectPermitted        EffectState = "permitted"
	EffectSucceeded        EffectState = "succeeded"
	EffectFailed           EffectState = "failed"
	EffectUnknown          EffectState = "unknown"
	EffectConfirmedNotSent EffectState = "confirmed_not_sent"
	EffectDenied           EffectState = "denied"
)

// EffectOutcome is the observed result of the permitted mutation.
type EffectOutcome string

const (
	EffectOutcomeSucceeded EffectOutcome = "succeeded"
	EffectOutcomeFailed    EffectOutcome = "failed"
	EffectOutcomeUnknown   EffectOutcome = "unknown"
)

// Effect is the durable business-write obligation and its single-use
// permit (DD-02 §5 business-write class, DD-06 §3).
type Effect struct {
	ID               string
	TenantID         string
	OperationID      string
	AttemptID        string
	Kind             EffectKind
	Occurrence       uint64
	CommandID        string
	Owner            string // the registered physical owner of the permit
	CanonicalSubject string
	RequestDigest    Digest
	ExpectedRevision string
	ExecutionEpoch   uint64
	RecoveryEpoch    uint64
	LeaseID          string
	LeaseFence       uint64
	LeaseExpiresAt   time.Time // zero when the mutation holds no lease
	State            EffectState
	Outcome          EffectOutcome
	DenialCode       string
	OutcomeRef       string
	QueryRef         string
	Inventory        InventoryState
	InventoryVersion string
	Deadline         time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	ObservedAt       *time.Time
}

// Unresolved reports whether the effect may still have an upstream effect
// whose outcome Control does not know: a consumed permit without a definite
// observation. Only such effects keep a recovery scope restricted.
func (e *Effect) Unresolved() bool {
	return e.State == EffectPermitted || e.State == EffectUnknown
}

// EffectRequest is what a sender submits for one guarded mutation: the
// operation (and attempt) it acts for, the physical owner, the mutation
// identity (kind, occurrence, canonical subject, expected upstream
// revision, request digest), its lease and the absolute deadline.
type EffectRequest struct {
	OperationID      string
	AttemptID        string
	Owner            string
	Kind             EffectKind
	Occurrence       uint64
	CanonicalSubject string
	ExpectedRevision string
	RequestDigest    Digest
	ExecutionEpoch   uint64
	LeaseID          string
	LeaseFence       uint64
	LeaseExpiresAt   time.Time
	Deadline         time.Time
}

// EffectContext is the current authoritative state the checks run against,
// loaded under Control's lock ranks by the application layer.
type EffectContext struct {
	Now             time.Time
	Operation       *Operation
	Attempt         *Attempt // nil when the request names no attempt
	AdmissionClosed bool     // the tenant's scope is closed by a recovery run
}

// DenyRecoveryRestricted is the denial code of a scope whose admission a
// recovery run closed; it is a STALE_EXECUTION outcome on the wire.
const DenyRecoveryRestricted = "RECOVERY_RESTRICTED"

// CheckEffect applies the permit rules (DD-06 §3, DD-02 §4 steps 2 and 4)
// to the request against the current state: tenant scope, admission
// closure, execution epoch and fences, attempt binding, deadlines that
// never extend the attempt's, and the lease fence. It returns a Denial for
// a policy refusal and ErrNotFound for an identity outside the caller's
// scope.
func CheckEffect(req EffectRequest, c EffectContext) error {
	op := c.Operation
	if op == nil {
		return fmt.Errorf("%w: operation %s", ErrNotFound, req.OperationID)
	}
	switch req.Kind {
	case EffectBusinessWrite, EffectPublication, EffectActivation, EffectReview:
	default:
		return fmt.Errorf("%w: effect kind %q", ErrInvalid, req.Kind)
	}
	if req.Owner == "" || req.CanonicalSubject == "" || req.Occurrence == 0 {
		return deny(DenyInvalidArgument, "effect needs an owner, a canonical subject and an occurrence")
	}
	if c.AdmissionClosed {
		return deny(DenyRecoveryRestricted, "admission for tenant %s is closed by a recovery run", op.TenantID)
	}
	if req.ExecutionEpoch != op.ExecutionEpoch {
		return deny(DenyStaleExecution, "binding epoch %d, operation epoch %d", req.ExecutionEpoch, op.ExecutionEpoch)
	}
	if op.FencedForNewDispatch() {
		return deny(DenyStaleExecution, "operation %s is fenced (%s/%s)", op.ID, op.Lifecycle, op.Control)
	}
	if !c.Now.Before(op.Deadline) {
		return deny(DenyStaleExecution, "operation %s deadline %s passed", op.ID, op.Deadline.UTC().Format(time.RFC3339))
	}
	deadlineBound := op.Deadline
	if req.AttemptID != "" {
		at := c.Attempt
		if at == nil || at.OperationID != op.ID {
			return fmt.Errorf("%w: attempt %s of operation %s", ErrNotFound, req.AttemptID, req.OperationID)
		}
		if at.ExecutionEpoch != op.ExecutionEpoch {
			return deny(DenyStaleExecution, "attempt epoch %d, operation epoch %d", at.ExecutionEpoch, op.ExecutionEpoch)
		}
		if at.State == AttemptClosed || at.State == AttemptResultAccepted {
			return deny(DenyStaleExecution, "attempt %s is %s", at.ID, at.State)
		}
		deadlineBound = at.Deadline
	}
	if !req.Deadline.After(c.Now) {
		return deny(DenyStaleExecution, "effect deadline %s passed", req.Deadline.UTC().Format(time.RFC3339))
	}
	if req.Deadline.After(deadlineBound) {
		return deny(DenyStaleExecution, "effect deadline %s extends the bound %s", req.Deadline.UTC().Format(time.RFC3339), deadlineBound.UTC().Format(time.RFC3339))
	}
	if req.LeaseID != "" && !req.LeaseExpiresAt.IsZero() && !req.LeaseExpiresAt.After(c.Now) {
		return deny(DenyStaleExecution, "lease %s (fence %d) expired at %s", req.LeaseID, req.LeaseFence, req.LeaseExpiresAt.UTC().Format(time.RFC3339))
	}
	return nil
}

// NewEffect records the prepared obligation after CheckEffect passed. The
// record keeps the epoch the request named: for a prepared effect it is
// the operation's (CheckEffect requires that), for a denial it is what the
// refused request said, so the identical request reenters its own denial
// instead of conflicting with the epoch it was refused for.
func NewEffect(req EffectRequest, c EffectContext, cmd CommandIdentity) *Effect {
	return &Effect{
		ID: NewID("eff"), TenantID: c.Operation.TenantID, OperationID: c.Operation.ID, AttemptID: req.AttemptID, Kind: req.Kind, Occurrence: req.Occurrence,
		CommandID: cmd.CommandID, Owner: req.Owner, CanonicalSubject: req.CanonicalSubject, RequestDigest: req.RequestDigest, ExpectedRevision: req.ExpectedRevision,
		ExecutionEpoch: req.ExecutionEpoch, RecoveryEpoch: c.Operation.RecoveryEpoch, LeaseID: req.LeaseID, LeaseFence: req.LeaseFence,
		LeaseExpiresAt: req.LeaseExpiresAt.UTC().Truncate(time.Microsecond), State: EffectPrepared, Inventory: InventoryPending,
		Deadline: req.Deadline.UTC().Truncate(time.Microsecond), CreatedAt: c.Now, UpdatedAt: c.Now,
	}
}

// DeniedEffect records a refusal under the command identity so a repeated
// request returns the same denial and never a permit.
func DeniedEffect(req EffectRequest, c EffectContext, cmd CommandIdentity, d *Denial) *Effect {
	e := NewEffect(req, c, cmd)
	e.State, e.DenialCode = EffectDenied, d.Code
	return e
}

// Request rebuilds the binding the effect was recorded with. The recheck
// before the permit is consumed runs against this persisted binding, never
// against the words of the request reentering it: a retry that clears or
// extends the lease expiry, or names another deadline, obtains nothing.
func (e *Effect) Request() EffectRequest {
	return EffectRequest{
		OperationID: e.OperationID, AttemptID: e.AttemptID, Owner: e.Owner, Kind: e.Kind, Occurrence: e.Occurrence, CanonicalSubject: e.CanonicalSubject,
		ExpectedRevision: e.ExpectedRevision, RequestDigest: e.RequestDigest, ExecutionEpoch: e.ExecutionEpoch, LeaseID: e.LeaseID, LeaseFence: e.LeaseFence,
		LeaseExpiresAt: e.LeaseExpiresAt, Deadline: e.Deadline,
	}
}

// Binds verifies that a request reentering the persisted effect is the
// same request: identity, owner, mutation, epoch, lease (identity, fence
// and expiry) and deadline. A request that keeps the command identity but
// names another binding is an idempotency conflict.
func (e *Effect) Binds(tenantID string, req EffectRequest, digest Digest) error {
	conflict := func(field string, recorded, offered any) error {
		return fmt.Errorf("%w: effect %s (%s #%d of %s) was prepared with %s %v, the request names %v", ErrIdempotencyConflict, e.ID, e.Kind, e.Occurrence, e.OperationID, field, recorded, offered)
	}
	switch {
	case e.TenantID != tenantID:
		return fmt.Errorf("%w: effect %s", ErrNotFound, e.ID)
	case e.RequestDigest != digest:
		return conflict("digest", e.RequestDigest, digest)
	case e.OperationID != req.OperationID:
		return conflict("operation", e.OperationID, req.OperationID)
	case e.AttemptID != req.AttemptID:
		return conflict("attempt", e.AttemptID, req.AttemptID)
	case e.Owner != req.Owner:
		return conflict("owner", e.Owner, req.Owner)
	case e.Kind != req.Kind || e.Occurrence != req.Occurrence:
		return conflict("mutation", fmt.Sprintf("%s#%d", e.Kind, e.Occurrence), fmt.Sprintf("%s#%d", req.Kind, req.Occurrence))
	case e.CanonicalSubject != req.CanonicalSubject:
		return conflict("subject", e.CanonicalSubject, req.CanonicalSubject)
	case e.ExpectedRevision != req.ExpectedRevision:
		return conflict("expected revision", e.ExpectedRevision, req.ExpectedRevision)
	case e.ExecutionEpoch != req.ExecutionEpoch:
		return conflict("execution epoch", e.ExecutionEpoch, req.ExecutionEpoch)
	case e.LeaseID != req.LeaseID || e.LeaseFence != req.LeaseFence:
		return conflict("lease", fmt.Sprintf("%s@%d", e.LeaseID, e.LeaseFence), fmt.Sprintf("%s@%d", req.LeaseID, req.LeaseFence))
	case !e.LeaseExpiresAt.Equal(req.LeaseExpiresAt.UTC().Truncate(time.Microsecond)):
		return conflict("lease expiry", e.LeaseExpiresAt.UTC().Format(time.RFC3339Nano), req.LeaseExpiresAt.UTC().Format(time.RFC3339Nano))
	case !e.Deadline.Equal(req.Deadline.UTC().Truncate(time.Microsecond)):
		return conflict("deadline", e.Deadline.UTC().Format(time.RFC3339Nano), req.Deadline.UTC().Format(time.RFC3339Nano))
	}
	return nil
}

// Permit consumes the single business-write permit: the only transition
// that returns permission to perform the mutation.
func (e *Effect) Permit(inventoryVersion string) error {
	if e.State != EffectPrepared {
		return fmt.Errorf("%w: effect %s is %s; the permit is issued once", ErrStaleExecution, e.ID, e.State)
	}
	e.State, e.Inventory, e.InventoryVersion = EffectPermitted, InventoryConfirmed, inventoryVersion
	return nil
}

// Deny records the refusal found by the revalidation.
func (e *Effect) Deny(code string) error {
	if e.State != EffectPrepared {
		return fmt.Errorf("%w: effect %s is %s", ErrStaleExecution, e.ID, e.State)
	}
	e.State, e.DenialCode = EffectDenied, code
	return nil
}

// Observe applies one outcome observation: a permitted or unknown effect
// takes the first definite outcome (its receipt reference becomes the
// outcome reference) or stays unknown. A settled effect keeps its first
// outcome; a later report is recorded by the caller as a duplicate and
// changes nothing. A definite outcome needs a receipt reference: an
// outcome without evidence of what happened upstream is unknown.
func (e *Effect) Observe(outcome EffectOutcome, receiptRef string, at time.Time) (settled bool, err error) {
	switch e.State {
	case EffectPermitted, EffectUnknown:
	case EffectSucceeded, EffectFailed:
		return false, nil
	default:
		return false, fmt.Errorf("%w: effect %s is %s; no permit was issued for a mutation", ErrStaleExecution, e.ID, e.State)
	}
	switch outcome {
	case EffectOutcomeUnknown:
		e.State, e.Outcome, e.ObservedAt = EffectUnknown, outcome, &at
		return false, nil
	case EffectOutcomeSucceeded, EffectOutcomeFailed:
		if receiptRef == "" {
			return false, fmt.Errorf("%w: effect %s reported %s without a receipt reference; report unknown otherwise", ErrEvidenceInsufficient, e.ID, outcome)
		}
	default:
		return false, fmt.Errorf("%w: effect outcome %q", ErrInvalid, outcome)
	}
	e.State, e.Outcome, e.OutcomeRef, e.ObservedAt = EffectState(outcome), outcome, receiptRef, &at
	return true, nil
}

// ConfirmNotSent closes a prepared, permitted or unknown effect on
// independently verified evidence that the mutation never reached the
// upstream; an effect with an observed outcome was sent.
func (e *Effect) ConfirmNotSent(evidenceRef string) error {
	switch e.State {
	case EffectConfirmedNotSent:
		if e.OutcomeRef != evidenceRef {
			return fmt.Errorf("%w: effect %s was confirmed not sent with evidence %q", ErrIdempotencyConflict, e.ID, e.OutcomeRef)
		}
		return nil
	case EffectPrepared, EffectPermitted, EffectUnknown:
	default:
		return fmt.Errorf("%w: effect %s is %s and cannot be confirmed not sent", ErrStaleExecution, e.ID, e.State)
	}
	e.State, e.OutcomeRef = EffectConfirmedNotSent, evidenceRef
	return nil
}

// EffectObservation is one durable outcome report of an effect; the same
// (source, sequence) is never applied twice.
type EffectObservation struct {
	ID              string
	EffectID        string
	Source          string
	Sequence        uint64
	Outcome         EffectOutcome
	ReceiptDigest   string
	NativeReference string
	ObservedAt      time.Time
}
