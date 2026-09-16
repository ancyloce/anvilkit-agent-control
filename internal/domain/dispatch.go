package domain

import (
	"errors"
	"fmt"
	"time"
)

// DispatchKind separates model sends (Model Proxy routes) from tool sends
// (MCP grants); both are single-use physical-send permissions (DD-02 §4).
type DispatchKind string

const (
	DispatchModel DispatchKind = "model"
	DispatchTool  DispatchKind = "tool"
)

// DispatchState is the admission state machine of one call:
//
//	prepared ──consume──▶ authorized ──observe──▶ observed
//	   │                      │                       ▲
//	   │                      └──observe(unknown)──▶ unknown ──observe──┘
//	   │                      │                       │
//	   │                      └────── evidence ──────▶ confirmed_not_sent ◀┘
//	   └──recheck fails──▶ denied
//
// AUTHORIZED means the permission was consumed, not that bytes were sent;
// only the request that moved prepared to authorized was told so. Nothing
// ever moves a call back to prepared or issues a second permission.
type DispatchState string

const (
	DispatchPrepared         DispatchState = "prepared"
	DispatchAuthorized       DispatchState = "authorized"
	DispatchObserved         DispatchState = "observed"
	DispatchUnknown          DispatchState = "unknown"
	DispatchConfirmedNotSent DispatchState = "confirmed_not_sent"
	DispatchDenied           DispatchState = "denied"
)

type DispatchOutcome string

const (
	DispatchSucceeded       DispatchOutcome = "succeeded"
	DispatchFailed          DispatchOutcome = "failed"
	DispatchCanceled        DispatchOutcome = "canceled"
	DispatchOutcomeUnknown  DispatchOutcome = "unknown"
	dispatchOutcomeNotKnown DispatchOutcome = ""
)

// Denial codes returned in Admission.denial_code; they are the public
// error vocabulary of contracts.md §4 applied to a recorded denial.
const (
	DenyStaleExecution     = "STALE_EXECUTION"
	DenyProfileUnqualified = "PROFILE_UNQUALIFIED"
	DenyForbidden          = "FORBIDDEN"
	DenyBudgetExhausted    = "BUDGET_EXHAUSTED"
	DenyInvalidArgument    = "INVALID_ARGUMENT"
)

// Dispatch is the durable model-dispatch or tool-dispatch obligation and
// its single-use permission.
type Dispatch struct {
	ID               string
	Kind             DispatchKind
	TenantID         string
	OperationID      string
	AttemptID        string
	InstanceID       string
	CallID           string
	Owner            string
	RequestDigest    Digest
	RouteID          string
	GrantID          string
	GrantRevision    uint64
	ExecutionEpoch   uint64
	State            DispatchState
	Outcome          DispatchOutcome
	DenialCode       string
	Reserved         Money
	MeterRevision    string
	SupersedesCallID string
	EvidenceRef      string
	Inventory        InventoryState
	InventoryVersion string
	Deadline         time.Time
	AdmittedAt       time.Time
	ObservedAt       *time.Time
}

// Terminal reports whether no permission can ever be issued for the call.
func (d *Dispatch) Terminal() bool {
	return d.State != DispatchPrepared
}

// Settled reports whether the reservation of the call has been released or
// converted: only an observed or confirmed-not-sent call no longer holds
// exposure.
func (d *Dispatch) Settled() bool {
	return d.State == DispatchObserved || d.State == DispatchConfirmedNotSent || d.State == DispatchDenied
}

// AdmissionRequest is what a sender submits (DD-02 §4 step 1): stable call
// identity, request digest, authenticated owner, execution binding, route
// or grant, deadline and maximum exposure.
type AdmissionRequest struct {
	Kind             DispatchKind
	CallID           string
	Owner            string
	RequestDigest    Digest
	OperationID      string
	AttemptID        string
	InstanceID       string
	ExecutionEpoch   uint64
	RouteID          string // model: the configured route; tool: server/method
	Provider         string // model: the provider the sender binds the call to
	Model            string // model: the model the sender binds the call to
	GrantID          string
	GrantRevision    uint64
	ServerID         string
	Method           string
	MaxExposure      Money
	Deadline         time.Time
	SupersedesCallID string
	EvidenceRef      string
}

// AdmissionContext is the current authoritative state the checks run
// against, loaded under Control's lock ranks by the application layer.
type AdmissionContext struct {
	Now         time.Time
	Operation   *Operation
	Attempt     *Attempt
	Instance    *Instance // nil when the binding names no instance
	Price       *Price    // nil when no reviewed price exists for the route
	Authority   Decision  // model routes: current commercial authority
	Grant       *GrantPolicy
	Allocations []*Allocation
	Overspent   bool
	Original    *Dispatch // the superseded call, when the request names one
	// AdmissionClosed: the tenant's scope is closed by a recovery run; no
	// new send permission is issued until it reopens.
	AdmissionClosed bool
}

// Denial is a recorded refusal: the public code and the reason.
type Denial struct {
	Code   string
	Reason string
}

func (d *Denial) Error() string { return d.Code + ": " + d.Reason }

func deny(code, format string, args ...any) *Denial {
	return &Denial{Code: code, Reason: fmt.Sprintf(format, args...)}
}

// CheckAdmission applies every rule of DD-02 §4 steps 2 and 4 to the
// request against the current state: identity and tenant, execution epoch
// and fences, deadlines, price, authority or grant, budget, and the
// replacement rule. It returns a Denial for a policy refusal, ErrNotFound
// for an identity outside the caller's scope and ErrInvalid for a request
// the schema could not reject. budget selects whether the allocation
// headroom is checked (step 2, before the reservation) or only the
// overspend fence (step 4, the exposure is already reserved).
func CheckAdmission(req AdmissionRequest, c AdmissionContext, budget bool) error {
	op, at := c.Operation, c.Attempt
	if op == nil || at == nil || at.OperationID != op.ID {
		return fmt.Errorf("%w: attempt %s of operation %s", ErrNotFound, req.AttemptID, req.OperationID)
	}
	if req.MaxExposure.Amount <= 0 {
		return deny(DenyInvalidArgument, "max exposure must be positive")
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
	if at.ExecutionEpoch != op.ExecutionEpoch {
		return deny(DenyStaleExecution, "attempt epoch %d, operation epoch %d", at.ExecutionEpoch, op.ExecutionEpoch)
	}
	if at.State == AttemptClosed || at.State == AttemptResultAccepted {
		return deny(DenyStaleExecution, "attempt %s is %s", at.ID, at.State)
	}
	if req.InstanceID != "" {
		if c.Instance == nil || c.Instance.AttemptID != at.ID {
			return deny(DenyStaleExecution, "instance %s is not an instance of attempt %s", req.InstanceID, at.ID)
		}
		if !c.Instance.Current {
			return deny(DenyStaleExecution, "instance %s is not the current physical owner", c.Instance.ID)
		}
	}
	if !c.Now.Before(at.Deadline) {
		return deny(DenyStaleExecution, "attempt %s deadline %s passed", at.ID, at.Deadline.UTC().Format(time.RFC3339))
	}
	if !req.Deadline.After(c.Now) {
		return deny(DenyStaleExecution, "call deadline %s passed", req.Deadline.UTC().Format(time.RFC3339))
	}
	if req.Deadline.After(at.Deadline) {
		return deny(DenyStaleExecution, "call deadline %s extends the attempt deadline %s", req.Deadline.UTC().Format(time.RFC3339), at.Deadline.UTC().Format(time.RFC3339))
	}
	if c.Price == nil {
		return deny(DenyProfileUnqualified, "no reviewed price for %s route %s", req.Kind, req.RouteID)
	}
	if c.Price.Kind != req.Kind || !c.Price.EffectiveAt(c.Now) {
		return deny(DenyProfileUnqualified, "price %s for route %s is not effective at %s", c.Price.Revision, req.RouteID, c.Now.UTC().Format(time.RFC3339))
	}
	if c.Price.Currency != req.MaxExposure.Currency {
		return deny(DenyProfileUnqualified, "route %s is priced in %s, exposure declared in %s", req.RouteID, c.Price.Currency, req.MaxExposure.Currency)
	}
	if req.MaxExposure.Amount > c.Price.MaxExposure {
		return deny(DenyProfileUnqualified, "exposure %s exceeds the route bound %d", req.MaxExposure, c.Price.MaxExposure)
	}
	switch req.Kind {
	case DispatchModel:
		// The route's observation binds the trusted provider and model; a
		// call that names neither, or another pair, has no reviewed price
		// on this route and the frozen revision could not meter it.
		if req.Provider == "" || req.Model == "" {
			return deny(DenyInvalidArgument, "model call on route %s binds no provider/model", req.RouteID)
		}
		if c.Price.Provider != req.Provider || c.Price.Model != req.Model {
			return deny(DenyProfileUnqualified, "route %s is priced for %s/%s (revision %s), the call binds %s/%s", req.RouteID, c.Price.Provider, c.Price.Model, c.Price.Revision, req.Provider, req.Model)
		}
		if !c.Authority.Current(c.Now) {
			reason := c.Authority.ReasonCode
			if reason == "" {
				reason = "no current authority"
			}
			return deny(DenyForbidden, "route %s: %s (revision %q, fresh until %s)", req.RouteID, reason, c.Authority.Revision, c.Authority.FreshUntil.UTC().Format(time.RFC3339))
		}
	case DispatchTool:
		if c.Grant == nil {
			return deny(DenyForbidden, "grant %s revision %d is not registered", req.GrantID, req.GrantRevision)
		}
		if err := c.Grant.Permits(op.TenantID, req.ServerID, req.Method, req.MaxExposure, c.Now); err != nil {
			return deny(DenyForbidden, "%v", err)
		}
	default:
		return fmt.Errorf("%w: dispatch kind %q", ErrInvalid, req.Kind)
	}
	if req.SupersedesCallID != "" {
		if c.Original == nil || c.Original.Owner != req.Owner || c.Original.TenantID != op.TenantID {
			return deny(DenyStaleExecution, "superseded call %s is not a call of this owner", req.SupersedesCallID)
		}
		if c.Original.State != DispatchConfirmedNotSent {
			return deny(DenyStaleExecution, "superseded call %s is %s, not confirmed not sent", req.SupersedesCallID, c.Original.State)
		}
		if req.EvidenceRef == "" || c.Original.EvidenceRef != req.EvidenceRef {
			return deny(DenyStaleExecution, "replacement must carry the evidence %q that confirmed call %s not sent", c.Original.EvidenceRef, req.SupersedesCallID)
		}
	}
	if c.Overspent {
		return deny(DenyBudgetExhausted, "operation %s has actual cost above its reserved exposure; new admission is blocked until the overspend is reviewed", op.ID)
	}
	if len(c.Allocations) == 0 {
		return deny(DenyBudgetExhausted, "operation %s has no budget allocation", op.ID)
	}
	if budget {
		for _, a := range c.Allocations {
			probe := *a
			if err := probe.Reserve(req.MaxExposure); err != nil {
				if errors.Is(err, ErrBudgetExhausted) {
					return deny(DenyBudgetExhausted, "%v", err)
				}
				return err
			}
		}
	}
	return nil
}

// NewDispatch records the prepared obligation after CheckAdmission passed
// and the exposure was reserved (step 2).
func NewDispatch(req AdmissionRequest, c AdmissionContext) *Dispatch {
	return &Dispatch{
		ID: NewID("dsp"), Kind: req.Kind, TenantID: c.Operation.TenantID, OperationID: c.Operation.ID, AttemptID: c.Attempt.ID, InstanceID: req.InstanceID,
		CallID: req.CallID, Owner: req.Owner, RequestDigest: req.RequestDigest, RouteID: req.RouteID, GrantID: req.GrantID, GrantRevision: req.GrantRevision,
		ExecutionEpoch: c.Operation.ExecutionEpoch, State: DispatchPrepared, Reserved: req.MaxExposure, MeterRevision: c.Price.Revision,
		SupersedesCallID: req.SupersedesCallID, EvidenceRef: req.EvidenceRef, Inventory: InventoryPending,
		Deadline: req.Deadline.UTC().Truncate(time.Microsecond), AdmittedAt: c.Now,
	}
}

// DeniedDispatch records a refusal under the call identity so a repeated
// request returns the same denial and never a permission. It keeps the
// epoch the refused request named (which may be the stale one it was
// refused for), so the identical request reenters its own denial and only
// a changed field conflicts.
func DeniedDispatch(req AdmissionRequest, c AdmissionContext, d *Denial) *Dispatch {
	meter := ""
	if c.Price != nil {
		meter = c.Price.Revision
	}
	return &Dispatch{
		ID: NewID("dsp"), Kind: req.Kind, TenantID: c.Operation.TenantID, OperationID: c.Operation.ID, AttemptID: c.Attempt.ID, InstanceID: req.InstanceID,
		CallID: req.CallID, Owner: req.Owner, RequestDigest: req.RequestDigest, RouteID: req.RouteID, GrantID: req.GrantID, GrantRevision: req.GrantRevision,
		ExecutionEpoch: req.ExecutionEpoch, State: DispatchDenied, DenialCode: d.Code, Reserved: Money{Currency: req.MaxExposure.Currency},
		MeterRevision: meter, SupersedesCallID: req.SupersedesCallID, EvidenceRef: req.EvidenceRef, Inventory: InventoryPending,
		Deadline: req.Deadline.UTC().Truncate(time.Microsecond), AdmittedAt: c.Now,
	}
}

// Binds verifies that a request reentering the persisted call is the same
// request: the identity (tenant, owner, call, digest) and every field the
// obligation was recorded with — kind, operation, attempt, instance, epoch,
// route or grant, exposure, deadline and replacement evidence. The persisted
// record is the authority; a request that keeps the identity but names
// another operation, attempt or binding is not a reentry and is refused as
// an idempotency conflict, so a fenced or expired call can never borrow
// another execution's authority. The frozen price, when the call has one,
// carries the trusted provider/model the request must still name.
func (d *Dispatch) Binds(tenantID string, req AdmissionRequest, frozen *Price) error {
	conflict := func(field string, recorded, offered any) error {
		return fmt.Errorf("%w: call %s of %s was admitted with %s %v, the request names %v", ErrIdempotencyConflict, d.CallID, d.Owner, field, recorded, offered)
	}
	switch {
	case d.TenantID != tenantID || d.Owner != req.Owner || d.CallID != req.CallID:
		return fmt.Errorf("%w: dispatch %s is not call %s of %s in tenant %s", ErrNotFound, d.ID, req.CallID, req.Owner, tenantID)
	case d.RequestDigest != req.RequestDigest:
		return conflict("digest", d.RequestDigest, req.RequestDigest)
	case d.Kind != req.Kind:
		return conflict("kind", d.Kind, req.Kind)
	case d.OperationID != req.OperationID:
		return conflict("operation", d.OperationID, req.OperationID)
	case d.AttemptID != req.AttemptID:
		return conflict("attempt", d.AttemptID, req.AttemptID)
	case d.InstanceID != req.InstanceID:
		return conflict("instance", d.InstanceID, req.InstanceID)
	case d.ExecutionEpoch != req.ExecutionEpoch:
		return conflict("execution epoch", d.ExecutionEpoch, req.ExecutionEpoch)
	case d.RouteID != req.RouteID:
		return conflict("route", d.RouteID, req.RouteID)
	case d.GrantID != req.GrantID || d.GrantRevision != req.GrantRevision:
		return conflict("grant", fmt.Sprintf("%s@%d", d.GrantID, d.GrantRevision), fmt.Sprintf("%s@%d", req.GrantID, req.GrantRevision))
	case d.Reserved.Currency != req.MaxExposure.Currency:
		return conflict("exposure currency", d.Reserved.Currency, req.MaxExposure.Currency)
	case d.State != DispatchDenied && d.Reserved.Amount != req.MaxExposure.Amount:
		// A denial reserves nothing, so only a prepared call records the amount.
		return conflict("exposure", d.Reserved.AmountString(), req.MaxExposure.AmountString())
	case !d.Deadline.Equal(req.Deadline.UTC().Truncate(time.Microsecond)):
		return conflict("deadline", d.Deadline.UTC().Format(time.RFC3339Nano), req.Deadline.UTC().Format(time.RFC3339Nano))
	case d.SupersedesCallID != req.SupersedesCallID:
		return conflict("superseded call", d.SupersedesCallID, req.SupersedesCallID)
	case d.State != DispatchConfirmedNotSent && d.EvidenceRef != req.EvidenceRef:
		// ConfirmNotSent overwrites the evidence of the call itself; the
		// replacement evidence is compared until then.
		return conflict("evidence", d.EvidenceRef, req.EvidenceRef)
	}
	// A denial was recorded for whatever provider/model the request named
	// (that mismatch may be the denial itself); only an admitted call was
	// bound to its route's observation.
	if d.State != DispatchDenied && frozen != nil && d.Kind == DispatchModel && (frozen.Provider != req.Provider || frozen.Model != req.Model) {
		return conflict("provider/model", frozen.Provider+"/"+frozen.Model, req.Provider+"/"+req.Model)
	}
	return nil
}

// Consume moves the prepared call to authorized: the single first-use
// permission (step 4). It is the only transition that returns permission.
func (d *Dispatch) Consume(inventoryVersion string) error {
	if d.State != DispatchPrepared {
		return fmt.Errorf("%w: dispatch %s is %s; permission is issued once", ErrStaleExecution, d.ID, d.State)
	}
	d.State, d.Inventory, d.InventoryVersion = DispatchAuthorized, InventoryConfirmed, inventoryVersion
	return nil
}

// Deny records the refusal found by the second transaction; the
// reservation is released by the caller.
func (d *Dispatch) Deny(code string) error {
	if d.State != DispatchPrepared {
		return fmt.Errorf("%w: dispatch %s is %s", ErrStaleExecution, d.ID, d.State)
	}
	d.State, d.DenialCode = DispatchDenied, code
	return nil
}

// Observe applies an outcome report: an authorized or unknown call becomes
// observed with a definite outcome (its reservation is then released by
// the caller) or unknown (exposure retained). An observed call keeps its
// first outcome; later reports only add usage. usageReported says whether
// the report carried cumulative counters: a definite outcome without them
// is insufficient settlement evidence (the exposure it would release is
// unknown, not zero) and is refused with ErrEvidenceInsufficient, leaving
// the call as it is; an unknown outcome needs no counters.
func (d *Dispatch) Observe(outcome DispatchOutcome, usageReported bool, at time.Time) (settled bool, err error) {
	switch d.State {
	case DispatchAuthorized, DispatchUnknown:
	case DispatchObserved:
		return false, nil
	default:
		return false, fmt.Errorf("%w: dispatch %s is %s; no permission was issued for a send", ErrStaleExecution, d.ID, d.State)
	}
	if outcome != DispatchOutcomeUnknown && !usageReported {
		return false, fmt.Errorf("%w: dispatch %s reported %s without usage; a definite outcome settles only with the metered usage (zero included), report unknown otherwise", ErrEvidenceInsufficient, d.ID, outcome)
	}
	if outcome == DispatchOutcomeUnknown {
		d.State, d.Outcome = DispatchUnknown, outcome
		d.ObservedAt = &at
		return false, nil
	}
	d.State, d.Outcome = DispatchObserved, outcome
	d.ObservedAt = &at
	return true, nil
}

// ConfirmNotSent closes an authorized, unknown or never-consumed call on
// independently verified evidence; a call whose usage was observed — cost
// charged, or counters explicitly reported even at zero — was sent and can
// never be confirmed not sent.
func (d *Dispatch) ConfirmNotSent(evidenceRef string, charged Money, usageReported bool) error {
	switch d.State {
	case DispatchConfirmedNotSent:
		if d.EvidenceRef != evidenceRef {
			return fmt.Errorf("%w: dispatch %s was confirmed not sent with evidence %q", ErrIdempotencyConflict, d.ID, d.EvidenceRef)
		}
		return nil
	case DispatchPrepared, DispatchAuthorized, DispatchUnknown:
	default:
		return fmt.Errorf("%w: dispatch %s is %s and cannot be confirmed not sent", ErrStaleExecution, d.ID, d.State)
	}
	if charged.Amount > 0 {
		return fmt.Errorf("%w: dispatch %s has %s of observed cost; it was sent", ErrStaleExecution, d.ID, charged)
	}
	if usageReported {
		return fmt.Errorf("%w: dispatch %s has explicitly reported usage; it was sent", ErrStaleExecution, d.ID)
	}
	d.State, d.EvidenceRef = DispatchConfirmedNotSent, evidenceRef
	return nil
}
