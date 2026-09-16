package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Dispatch implements budgets and the single-use physical-send permission
// (DD-02 §3–§4, delivery.md P06): platform/tenant/actor allocations,
// inventory-before-permission admission in two ordered transactions with
// the obligation written between them, deduplicated cumulative metering
// with append-only corrections, and not-sent confirmation on independent
// evidence. No network, inventory or storage I/O happens while a database
// lock is held.
type Dispatch struct {
	store     Store
	inventory Inventory
	prices    PriceBook
	authority Authority
	evidence  NotSentEvidence
	clock     domain.Clock
	log       *slog.Logger
}

func NewDispatch(store Store, inventory Inventory, prices PriceBook, authority Authority, evidence NotSentEvidence, clock domain.Clock, log *slog.Logger) *Dispatch {
	return &Dispatch{store: store, inventory: inventory, prices: prices, authority: authority, evidence: evidence, clock: clock, log: log}
}

// Allocate gives the operation its share of the current platform, tenant
// and actor pools (DD-02 §3): one allocation per level, each within its
// pool's shared cap, under the pool locks (rank 1) taken before the
// operation lock (rank 2). A repeated call returns the existing
// allocations; a missing pool at any level or a cap that cannot cover the
// amount denies with ErrBudgetExhausted and allocates nothing.
func (s *Dispatch) Allocate(ctx context.Context, scope domain.Scope, operationID string, amount domain.Money) (allocs []*domain.Allocation, existing bool, err error) {
	now := s.clock.Now()
	err = s.store.Tx(ctx, func(r Repo) error {
		pools, err := r.ListCurrentPools(ctx, amount.Currency, scope.TenantID, scope.ActorID, now)
		if err != nil {
			return err
		}
		byLevel := map[domain.BudgetLevel]*domain.BudgetPool{}
		for _, p := range pools {
			if byLevel[p.Level] != nil {
				return fmt.Errorf("%w: more than one current %s pool", domain.ErrInvalid, p.Level)
			}
			byLevel[p.Level] = p
		}
		for _, level := range domain.BudgetLevels {
			if byLevel[level] == nil {
				return fmt.Errorf("%w: no current %s pool in %s for the scope", domain.ErrBudgetExhausted, level, amount.Currency)
			}
		}
		locked := make([]*domain.BudgetPool, 0, len(pools))
		for _, p := range pools { // already ordered by pool id
			lp, err := r.LockPool(ctx, p.ID)
			if err != nil {
				return err
			}
			locked = append(locked, lp)
		}
		op, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if op.TenantID != scope.TenantID {
			return domain.ErrNotFound
		}
		if op.FencedForNewDispatch() {
			return fmt.Errorf("%w: operation %s is fenced", domain.ErrStaleExecution, op.ID)
		}
		var fresh []*domain.Allocation
		existing = true
		for _, p := range locked {
			found, err := r.GetAllocationByPool(ctx, p.ID, op.ID)
			switch {
			case err == nil:
				allocs = append(allocs, found)
				continue
			case !errors.Is(err, domain.ErrNotFound):
				return err
			}
			existing = false
			if err := p.Allocate(amount, now); err != nil {
				return err
			}
			a := domain.NewAllocation(p, op.ID, amount, now)
			fresh = append(fresh, a)
			allocs = append(allocs, a)
		}
		if len(fresh) == 0 {
			return nil
		}
		for _, p := range locked {
			if err := r.UpdatePool(ctx, p); err != nil {
				return err
			}
		}
		for _, a := range fresh {
			if err := r.InsertAllocation(ctx, a); err != nil {
				return err
			}
		}
		ev := op.Transition("allocation:"+fresh[0].ID, now, func(o *domain.Operation) { o.Finance = domain.FinanceFunded })
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		return r.InsertEvent(ctx, ev)
	})
	if err != nil {
		return nil, false, err
	}
	return allocs, existing, nil
}

// Admission is the answer to one admission request: the dispatch record,
// whether this request consumed the single first-use permission, and the
// denial code when the call is (or was) refused.
type Admission struct {
	Dispatch   *domain.Dispatch
	Allowed    bool
	DenialCode string
}

// Admit is the single-use admission of DD-02 §4 for model and tool calls.
//
//  1. Current authority for the route is read outside any lock.
//  2. One transaction, in lock order (allocations, operation, attempt,
//     instance, dispatch), verifies identity, epoch, fences, deadlines,
//     price (for a model route the trusted provider/model it binds),
//     authority or grant, budget and the replacement rule; the same call
//     and digest returns the existing record (no permission), a changed
//     digest or a changed binding conflicts, a refusal is recorded as
//     denied, and the first eligible request reserves the exposure and
//     records PREPARED.
//  3. Outside the transaction the immutable obligation is written; an
//     uncertain write is recorded and grants nothing.
//  4. A second transaction reacquires the locks of the persisted call's
//     own operation, attempt and instance (never the request's word for
//     them), verifies that the locked record is the caller's call with the
//     same binding, revalidates authority, fences, deadlines, the frozen
//     price and budget, verifies the object version and consumes the
//     permission: only the request completing that transition is told
//     dispatch_allowed=true. A request that finds the call already
//     authorized (its own lost first response included) is never told so
//     again; a request reentering a PREPARED call (its inventory write
//     failed or was lost) completes the same sequence once.
func (s *Dispatch) Admit(ctx context.Context, cmd domain.CommandIdentity, req domain.AdmissionRequest) (Admission, error) {
	if req.RequestDigest == "" {
		req.RequestDigest = cmd.RequestDigest
	}
	scope := domain.Scope{TenantID: cmd.TenantID, ActorID: cmd.ActorID}
	authority, err := s.currentAuthority(ctx, scope, req)
	if err != nil {
		return Admission{}, err
	}
	var d *domain.Dispatch
	proceed := false
	for attempt := 0; attempt < 2; attempt++ {
		err = s.store.Tx(ctx, func(r Repo) error {
			found, err := r.GetDispatchByCall(ctx, cmd.TenantID, req.Owner, req.CallID)
			switch {
			case err == nil:
				// The persisted record is the call: a request that keeps the
				// identity but names another binding is refused before it
				// can act on this record.
				if err := found.Binds(cmd.TenantID, req, s.frozenPrice(found)); err != nil {
					return err
				}
				d, proceed = found, found.State == domain.DispatchPrepared
				return nil
			case !errors.Is(err, domain.ErrNotFound):
				return err
			}
			ac, err := s.admissionContext(ctx, r, requestBinding(req), cmd.TenantID, authority)
			if err != nil {
				return err
			}
			now := ac.Now
			if p, ok := s.prices.Price(req.Kind, req.RouteID, now); ok {
				ac.Price = p
			}
			if err := domain.CheckAdmission(req, ac, true); err != nil {
				var denial *domain.Denial
				if !errors.As(err, &denial) {
					return err
				}
				d = domain.DeniedDispatch(req, ac, denial)
				return r.InsertDispatch(ctx, d)
			}
			fresh := domain.NewDispatch(req, ac)
			for _, a := range ac.Allocations {
				if err := a.Reserve(req.MaxExposure); err != nil {
					return err
				}
				if err := r.UpdateAllocation(ctx, a); err != nil {
					return err
				}
			}
			if err := r.InsertDispatch(ctx, fresh); err != nil {
				return err
			}
			if err := r.InsertCostEntry(ctx, &domain.CostEntry{ID: domain.NewID("cost"), OperationID: fresh.OperationID, DispatchID: fresh.ID, Kind: domain.CostEstimate, Amount: fresh.Reserved, CreatedAt: now}); err != nil {
				return err
			}
			d, proceed = fresh, true
			return nil
		})
		if errors.Is(err, ErrDuplicateKey) && attempt == 0 {
			continue // another replica prepared the same call first; reload it
		}
		break
	}
	if err != nil {
		return Admission{}, err
	}
	if !proceed {
		return Admission{Dispatch: d, Allowed: false, DenialCode: d.DenialCode}, nil
	}

	// Step 3: the immutable obligation, outside every lock. Same key and
	// body is idempotent across replicas and retries.
	key, body := dispatchObligation(d)
	version, err := s.inventory.Put(ctx, key, body)
	if err != nil {
		if merr := s.markInventoryUncertain(ctx, d.ID); merr != nil {
			s.log.Warn("dispatch inventory state not recorded", "dispatchId", d.ID, "error", merr)
		}
		class, _, _ := ParseObligationKey(key)
		return Admission{}, fmt.Errorf("%w: %s inventory write for dispatch %s: %v", domain.ErrEffectUncertain, class, d.ID, err)
	}

	// Step 4: revalidate under the locks of the persisted call and consume
	// the permission once. The context is loaded from the record's own
	// binding, the locked record must still be this caller's call, and the
	// price is the observation frozen at step 2, never a later revision.
	authority, err = s.currentAuthority(ctx, scope, req)
	if err != nil {
		return Admission{}, err
	}
	allowed := false
	err = s.store.Tx(ctx, func(r Repo) error {
		ac, err := s.admissionContext(ctx, r, recordBinding(d), cmd.TenantID, authority)
		if err != nil {
			return err
		}
		locked, err := r.LockDispatch(ctx, d.ID)
		if err != nil {
			return err
		}
		d = locked
		if err := locked.Binds(cmd.TenantID, req, nil); err != nil {
			return err
		}
		if locked.State != domain.DispatchPrepared {
			return nil // consumed by another request; never a second permission
		}
		// The clock is read with every lock held: deadlines are checked as
		// of the decision, not as of a sample taken before lock contention.
		ac.Now = s.clock.Now()
		frozen, ok := s.prices.PriceByRevision(locked.MeterRevision)
		if !ok {
			return fmt.Errorf("%w: dispatch %s was prepared under price revision %q, which is no longer available", domain.ErrProfileUnqualified, locked.ID, locked.MeterRevision)
		}
		ac.Price = frozen
		if err := domain.CheckAdmission(req, ac, false); err != nil {
			var denial *domain.Denial
			if !errors.As(err, &denial) {
				return err
			}
			if err := locked.Deny(denial.Code); err != nil {
				return err
			}
			for _, a := range ac.Allocations {
				if err := a.Release(locked.Reserved); err != nil {
					return err
				}
				if err := r.UpdateAllocation(ctx, a); err != nil {
					return err
				}
			}
			return r.UpdateDispatch(ctx, locked)
		}
		if err := locked.Consume(version); err != nil {
			return err
		}
		if err := r.UpdateDispatch(ctx, locked); err != nil {
			return err
		}
		allowed = true
		return nil
	})
	if err != nil {
		return Admission{}, err
	}
	return Admission{Dispatch: d, Allowed: allowed, DenialCode: d.DenialCode}, nil
}

func (s *Dispatch) currentAuthority(ctx context.Context, scope domain.Scope, req domain.AdmissionRequest) (domain.Decision, error) {
	if req.Kind != domain.DispatchModel {
		return domain.Decision{}, nil
	}
	decision, err := s.authority.CheckExecution(ctx, scope, req.RouteID)
	if err != nil {
		return domain.Decision{}, fmt.Errorf("authority for route %s: %w", req.RouteID, err)
	}
	return decision, nil
}

// binding is the execution identity an admission context is loaded for:
// the request's own words for a fresh call (step 2), the persisted
// record's for a reentry and the consuming transaction (step 4).
type binding struct {
	Kind             domain.DispatchKind
	Owner            string
	OperationID      string
	AttemptID        string
	InstanceID       string
	GrantID          string
	GrantRevision    uint64
	SupersedesCallID string
}

func requestBinding(r domain.AdmissionRequest) binding {
	return binding{Kind: r.Kind, Owner: r.Owner, OperationID: r.OperationID, AttemptID: r.AttemptID, InstanceID: r.InstanceID, GrantID: r.GrantID, GrantRevision: r.GrantRevision, SupersedesCallID: r.SupersedesCallID}
}

func recordBinding(d *domain.Dispatch) binding {
	return binding{Kind: d.Kind, Owner: d.Owner, OperationID: d.OperationID, AttemptID: d.AttemptID, InstanceID: d.InstanceID, GrantID: d.GrantID, GrantRevision: d.GrantRevision, SupersedesCallID: d.SupersedesCallID}
}

// frozenPrice is the observation a recorded call was metered under, nil
// for a denial that never had one.
func (s *Dispatch) frozenPrice(d *domain.Dispatch) *domain.Price {
	if d.MeterRevision == "" {
		return nil
	}
	p, _ := s.prices.PriceByRevision(d.MeterRevision)
	return p
}

// admissionContext loads the current state under Control's lock ranks:
// allocations (1), operation (2), attempt and instance (3). The dispatch
// itself (4) is locked by the caller when it exists; the price is chosen by
// the caller (current at step 2, frozen at step 4). Now is read after the
// locks are held.
func (s *Dispatch) admissionContext(ctx context.Context, r Repo, b binding, tenantID string, authority domain.Decision) (domain.AdmissionContext, error) {
	allocs, err := r.LockAllocations(ctx, b.OperationID)
	if err != nil {
		return domain.AdmissionContext{}, err
	}
	op, err := r.LockOperation(ctx, b.OperationID)
	if err != nil {
		return domain.AdmissionContext{}, err
	}
	if op.TenantID != tenantID {
		return domain.AdmissionContext{}, domain.ErrNotFound
	}
	at, err := r.LockAttempt(ctx, b.AttemptID)
	if err != nil {
		return domain.AdmissionContext{}, err
	}
	if at.OperationID != op.ID {
		return domain.AdmissionContext{}, domain.ErrNotFound
	}
	closed, err := r.IsAdmissionClosed(ctx, op.TenantID)
	if err != nil {
		return domain.AdmissionContext{}, err
	}
	ac := domain.AdmissionContext{Operation: op, Attempt: at, Allocations: allocs, Authority: authority, AdmissionClosed: closed}
	if b.InstanceID != "" {
		inst, err := r.LockInstance(ctx, b.InstanceID)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return domain.AdmissionContext{}, err
		}
		ac.Instance = inst
	}
	if b.Kind == domain.DispatchTool {
		g, err := r.GetGrantPolicy(ctx, b.GrantID, b.GrantRevision)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return domain.AdmissionContext{}, err
		}
		ac.Grant = g
	}
	if b.SupersedesCallID != "" {
		orig, err := r.GetDispatchByCall(ctx, tenantID, b.Owner, b.SupersedesCallID)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return domain.AdmissionContext{}, err
		}
		ac.Original = orig
	}
	overspent, err := r.HasOverspend(ctx, op.ID)
	if err != nil {
		return domain.AdmissionContext{}, err
	}
	ac.Overspent = overspent
	ac.Now = s.clock.Now()
	return ac, nil
}

func (s *Dispatch) markInventoryUncertain(ctx context.Context, dispatchID string) error {
	return s.store.Tx(ctx, func(r Repo) error {
		d, err := r.LockDispatch(ctx, dispatchID)
		if err != nil {
			return err
		}
		if d.State != domain.DispatchPrepared || d.Inventory != domain.InventoryPending {
			return nil
		}
		d.Inventory = domain.InventoryUncertain
		return r.UpdateDispatch(ctx, d)
	})
}

// Get returns the dispatch record by id or by owner and call id; it never
// returns permission.
func (s *Dispatch) Get(ctx context.Context, dispatchID, owner, callID string) (*domain.Dispatch, error) {
	var d *domain.Dispatch
	err := s.store.Read(ctx, func(r Repo) error {
		var err error
		if dispatchID != "" {
			d, err = r.GetDispatch(ctx, dispatchID)
		} else if owner != "" && callID != "" {
			d, err = r.FindDispatchByOwnerCall(ctx, owner, callID)
		} else {
			err = fmt.Errorf("%w: a dispatch id or an owner and call id is required", domain.ErrInvalid)
		}
		return err
	})
	return d, err
}

// Observe applies one idempotent outcome/usage report (DD-02 §3–§4 step 5).
// The same (source, sequence) is never applied twice; usage is cumulative
// per source and priced under the dispatch's meter revision as the running
// maximum across sources, so a duplicate or out-of-order report charges
// nothing and every increase is one append-only actual or correction entry.
// Actual cost above the reservation is retained (overspend fences new
// admission of the operation); a definite outcome releases the rest of the
// reservation, an unknown outcome retains all of it and marks the
// operation's exposure unknown.
//
// usage is nil when the report carried no counters. A definite outcome
// without them is insufficient settlement evidence — what it would release
// is unknown exposure, not zero cost — and is refused with
// ErrEvidenceInsufficient; nothing is recorded, the call keeps its state and
// reservation, and the sender reports again with the metered usage (zero
// included) or reports unknown. An unknown outcome without counters is
// recorded for deduplication, retains the exposure and meters nothing.
func (s *Dispatch) Observe(ctx context.Context, dispatchID, source string, sequence uint64, outcome domain.DispatchOutcome, usage *domain.Usage, nativeRef string, observedAt time.Time) (d *domain.Dispatch, existing bool, err error) {
	now := s.clock.Now()
	var peek *domain.Dispatch
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		peek, err = r.GetDispatch(ctx, dispatchID)
		return err
	}); err != nil {
		return nil, false, err
	}
	price, ok := s.prices.PriceByRevision(peek.MeterRevision)
	if !ok {
		return nil, false, fmt.Errorf("%w: dispatch %s was metered under price revision %q, which is no longer available", domain.ErrProfileUnqualified, peek.ID, peek.MeterRevision)
	}
	err = s.store.Tx(ctx, func(r Repo) error {
		allocs, err := r.LockAllocations(ctx, peek.OperationID)
		if err != nil {
			return err
		}
		op, err := r.LockOperation(ctx, peek.OperationID)
		if err != nil {
			return err
		}
		locked, err := r.LockDispatch(ctx, dispatchID)
		if err != nil {
			return err
		}
		d = locked
		if _, err := r.GetUsageObservation(ctx, locked.ID, source, sequence); err == nil {
			existing = true
			return nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		if locked.State == domain.DispatchPrepared || locked.State == domain.DispatchDenied || locked.State == domain.DispatchConfirmedNotSent {
			return fmt.Errorf("%w: dispatch %s is %s; no permission was issued for a send", domain.ErrStaleExecution, locked.ID, locked.State)
		}
		// The outcome rule runs before anything is written: a refused report
		// leaves no observation behind, so the sender's corrected report under
		// the same sequence is not a duplicate.
		settled, err := locked.Observe(outcome, usage != nil, observedAt)
		if err != nil {
			return err
		}
		obs := &domain.UsageObservation{ID: domain.NewID("obs"), DispatchID: locked.ID, Source: source, Sequence: sequence, NativeReference: nativeRef, Usage: usage, CostRevision: price.Revision, ObservedAt: observedAt}
		if err := r.InsertUsageObservation(ctx, obs); err != nil {
			return err
		}
		cumulative, err := r.MaxUsage(ctx, locked.ID)
		if err != nil {
			return err
		}
		cost, err := price.Cost(cumulative)
		if err != nil {
			return err
		}
		charged, actualID, err := r.ChargedCost(ctx, locked.ID)
		if err != nil {
			return err
		}
		if delta := cost.Amount - charged; delta > 0 {
			entry := &domain.CostEntry{ID: domain.NewID("cost"), OperationID: locked.OperationID, DispatchID: locked.ID, ObservationID: obs.ID, Kind: domain.CostActual, Amount: domain.Money{Currency: cost.Currency, Amount: delta}, CreatedAt: now}
			if actualID != "" {
				entry.Kind, entry.CorrectionOf = domain.CostCorrection, actualID
			}
			if err := r.InsertCostEntry(ctx, entry); err != nil {
				return err
			}
			for _, a := range allocs {
				if err := a.Charge(entry.Amount, locked.Reserved, false); err != nil {
					return err
				}
			}
			if cost.Amount > locked.Reserved.Amount {
				s.log.Warn("dispatch overspend retained; new admission of the operation is blocked", "dispatchId", locked.ID, "operationId", locked.OperationID, "reserved", locked.Reserved.String(), "actual", cost.String())
			}
		}
		if settled {
			for _, a := range allocs {
				if err := a.Release(locked.Reserved); err != nil {
					return err
				}
			}
		}
		for _, a := range allocs {
			if err := r.UpdateAllocation(ctx, a); err != nil {
				return err
			}
		}
		if err := r.UpdateDispatch(ctx, locked); err != nil {
			return err
		}
		return s.settleFinance(ctx, r, op, locked, settled, now)
	})
	return d, existing, err
}

// settleFinance keeps the operation's finance state truthful: any
// dispatch left unknown marks the exposure unknown; once none remains the
// operation is funded again. Both are committed with an event.
func (s *Dispatch) settleFinance(ctx context.Context, r Repo, op *domain.Operation, d *domain.Dispatch, settled bool, now time.Time) error {
	switch {
	case d.State == domain.DispatchUnknown && op.Finance != domain.FinanceExposureUnknown:
		ev := op.Transition("dispatch:"+d.ID+":unknown", now, func(o *domain.Operation) { o.Finance = domain.FinanceExposureUnknown })
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		return r.InsertEvent(ctx, ev)
	case settled && op.Finance == domain.FinanceExposureUnknown:
		unknown, err := r.HasUnknownDispatch(ctx, op.ID)
		if err != nil || unknown {
			return err
		}
		ev := op.Transition("dispatch:"+d.ID+":settled", now, func(o *domain.Operation) { o.Finance = domain.FinanceFunded })
		if err := r.UpdateOperation(ctx, op); err != nil {
			return err
		}
		return r.InsertEvent(ctx, ev)
	}
	return nil
}

// ConfirmNotSent closes a call whose send never happened on independently
// checkable evidence (DD-02 §4): the evidence is verified outside locks
// against the dispatch, then the call becomes confirmed_not_sent and its
// reservation is released. A call with observed usage — cost charged, or
// counters explicitly reported even at zero — was sent and is refused;
// insufficient evidence leaves the call as it is.
func (s *Dispatch) ConfirmNotSent(ctx context.Context, cmd domain.CommandIdentity, dispatchID, evidenceRef string, evidenceDigest domain.Digest) (d *domain.Dispatch, existing bool, err error) {
	var peek *domain.Dispatch
	if err := s.store.Read(ctx, func(r Repo) error {
		var err error
		peek, err = r.GetDispatch(ctx, dispatchID)
		return err
	}); err != nil {
		return nil, false, err
	}
	if peek.TenantID != cmd.TenantID {
		return nil, false, domain.ErrNotFound
	}
	if peek.State == domain.DispatchConfirmedNotSent {
		if peek.EvidenceRef != evidenceRef {
			return nil, false, fmt.Errorf("%w: dispatch %s was confirmed not sent with evidence %q", domain.ErrIdempotencyConflict, peek.ID, peek.EvidenceRef)
		}
		return peek, true, nil
	}
	if err := s.evidence.VerifyNotSent(ctx, peek, evidenceRef, evidenceDigest); err != nil {
		return nil, false, err
	}
	err = s.store.Tx(ctx, func(r Repo) error {
		var err error
		d, existing, err = s.ConfirmNotSentLocked(ctx, r, peek.OperationID, dispatchID, evidenceRef, nil)
		return err
	})
	return d, existing, err
}

// ConfirmNotSentLocked applies the not-sent confirmation inside the
// caller's transaction, taking the locks in rank order (allocations,
// operation, dispatch); guard, when given, runs under the operation lock
// before anything changes, so a caller that binds the confirmation to a
// current authority (a recovery epoch) refuses without a partial change.
// The evidence must have been verified outside the transaction.
func (s *Dispatch) ConfirmNotSentLocked(ctx context.Context, r Repo, operationID, dispatchID, evidenceRef string, guard func(*domain.Operation) error) (d *domain.Dispatch, existing bool, err error) {
	allocs, err := r.LockAllocations(ctx, operationID)
	if err != nil {
		return nil, false, err
	}
	op, err := r.LockOperation(ctx, operationID)
	if err != nil {
		return nil, false, err
	}
	if guard != nil {
		if err := guard(op); err != nil {
			return nil, false, err
		}
	}
	locked, err := r.LockDispatch(ctx, dispatchID)
	if err != nil {
		return nil, false, err
	}
	now := s.clock.Now()
	if locked.State == domain.DispatchConfirmedNotSent {
		return locked, true, locked.ConfirmNotSent(evidenceRef, domain.Money{Currency: locked.Reserved.Currency}, false)
	}
	charged, _, err := r.ChargedCost(ctx, locked.ID)
	if err != nil {
		return nil, false, err
	}
	reported, err := r.HasReportedUsage(ctx, locked.ID)
	if err != nil {
		return nil, false, err
	}
	held := locked.State == domain.DispatchAuthorized || locked.State == domain.DispatchUnknown || locked.State == domain.DispatchPrepared
	if err := locked.ConfirmNotSent(evidenceRef, domain.Money{Currency: locked.Reserved.Currency, Amount: charged}, reported); err != nil {
		return nil, false, err
	}
	if held {
		for _, a := range allocs {
			if err := a.Release(locked.Reserved); err != nil {
				return nil, false, err
			}
			if err := r.UpdateAllocation(ctx, a); err != nil {
				return nil, false, err
			}
		}
	}
	if err := r.UpdateDispatch(ctx, locked); err != nil {
		return nil, false, err
	}
	return locked, false, s.settleFinance(ctx, r, op, locked, true, now)
}
