package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// Finance is the operator administration of the price book, the budget
// pools and the cost caps, and the financial settlement of terminal
// operations (P2.1, DD-02 §3, ENV-06). Every mutation is decided on the
// operator's verified roles (P0.3): money-setting commands need the
// platform_operator role; an operator of a tenant may read that tenant's
// pools and caps.
type Finance struct {
	store   Store
	refresh func(context.Context) error
	clock   domain.Clock
	log     *slog.Logger
}

// NewFinance builds the use case; refresh, when set, reloads the local
// price snapshot after a publication or withdrawal commits.
func NewFinance(store Store, refresh func(context.Context) error, clock domain.Clock, log *slog.Logger) *Finance {
	return &Finance{store: store, refresh: refresh, clock: clock, log: log}
}

// Finance actions named in refusals.
const (
	ActionPublishPrice = "publish a price revision"
	ActionFundPool     = "fund or adjust a budget pool"
	ActionSetCostCap   = "set a cost cap"
)

// financeWriter authorizes a money-setting command: the scope is the
// command's own tenant and actor and holds the platform_operator role.
func financeWriter(scope domain.Scope, cmd domain.CommandIdentity, action string) error {
	if scope.TenantID != cmd.TenantID || scope.ActorID != cmd.ActorID {
		return fmt.Errorf("%w: the operator scope is not the command's tenant and actor", domain.ErrForbidden)
	}
	if !scope.HasRole(domain.RolePlatformOperator) {
		return fmt.Errorf("%w: actor %s may not %s: the %s role is required", domain.ErrForbidden, cmd.ActorID, action, domain.RolePlatformOperator)
	}
	return nil
}

// financeReader authorizes a read of a tenant's finance records (tenantID
// empty: platform-wide records such as prices and platform pools).
func financeReader(scope domain.Scope, tenantID string) error {
	if scope.HasRole(domain.RolePlatformOperator) {
		return nil
	}
	if scope.HasRole(domain.RoleOperator) && (tenantID == "" || tenantID == scope.TenantID) {
		return nil
	}
	return fmt.Errorf("%w: actor %s may not read finance records of %q", domain.ErrForbidden, scope.ActorID, tenantID)
}

func (f *Finance) refreshed(ctx context.Context) {
	if f.refresh == nil {
		return
	}
	if err := f.refresh(ctx); err != nil {
		f.log.Warn("price book refresh after a publication failed; the next interval refreshes it", "error", err)
	}
}

// PublishPrice records an immutable price revision. The same revision with
// the same content returns the existing record; other content under the
// revision is an idempotency conflict.
func (f *Finance) PublishPrice(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, p domain.Price) (rec *domain.PriceRecord, existing bool, err error) {
	if err := financeWriter(scope, cmd, ActionPublishPrice); err != nil {
		return nil, false, err
	}
	return f.publish(ctx, cmd, p)
}

// InstallPrices publishes the reviewed ENV-06 price book a deployment names
// (dispatch.prices.initial_file) at start, each revision idempotently under
// Control's own identity; a revision already published with other content
// refuses the start.
func (f *Finance) InstallPrices(ctx context.Context, prices []domain.Price) (published int, err error) {
	for _, p := range prices {
		_, existing, err := f.publish(ctx, domain.CommandIdentity{TenantID: "*", CommandID: "initial-price-book", ActorID: "anvilkit-agent-control"}, p)
		if err != nil {
			return published, fmt.Errorf("price revision %s: %w", p.Revision, err)
		}
		if !existing {
			published++
		}
	}
	return published, nil
}

func (f *Finance) publish(ctx context.Context, cmd domain.CommandIdentity, p domain.Price) (rec *domain.PriceRecord, existing bool, err error) {
	p.EffectiveFrom, p.EffectiveUntil = p.EffectiveFrom.UTC().Truncate(time.Microsecond), p.EffectiveUntil.UTC().Truncate(time.Microsecond)
	if err := p.Validate(); err != nil {
		return nil, false, err
	}
	digest := domain.PriceDigest(p)
	err = f.store.Tx(ctx, func(r Repo) error {
		found, err := r.GetPriceRevision(ctx, p.Revision)
		switch {
		case err == nil:
			if found.Digest != digest {
				return fmt.Errorf("%w: price revision %s was published with other content", domain.ErrIdempotencyConflict, p.Revision)
			}
			rec, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		rec = &domain.PriceRecord{Price: p, Digest: digest, PublishedBy: cmd.ActorID, CommandID: cmd.CommandID, PublishedAt: f.clock.Now().UTC().Truncate(time.Microsecond)}
		return r.InsertPriceRevision(ctx, rec)
	})
	if errors.Is(err, ErrDuplicateKey) && depthOf(ctx) < duplicateRetries {
		return f.publish(deeper(ctx), cmd, p)
	}
	if err != nil {
		return nil, false, err
	}
	if !existing {
		f.log.Info("price revision published", "revision", rec.Revision, "kind", rec.Kind, "route", rec.Route, "digest", rec.Digest, "actor", cmd.ActorID)
		f.refreshed(ctx)
	}
	return rec, existing, nil
}

// WithdrawPrice stops new admission under the revision; dispatches already
// prepared under it keep their frozen price until they settle.
func (f *Finance) WithdrawPrice(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, revision, reason string) (rec *domain.PriceRecord, existing bool, err error) {
	if err := financeWriter(scope, cmd, ActionPublishPrice); err != nil {
		return nil, false, err
	}
	now := f.clock.Now().UTC().Truncate(time.Microsecond)
	err = f.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockPriceRevision(ctx, revision)
		if err != nil {
			return err
		}
		if locked.WithdrawnAt != nil {
			rec, existing = locked, true
			return nil
		}
		if err := r.WithdrawPriceRevision(ctx, revision, now, cmd.ActorID); err != nil {
			return err
		}
		locked.WithdrawnAt, locked.WithdrawnBy = &now, cmd.ActorID
		rec = locked
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if !existing {
		f.log.Info("price revision withdrawn", "revision", revision, "actor", cmd.ActorID, "reason", reason)
		f.refreshed(ctx)
	}
	return rec, existing, nil
}

func (f *Finance) GetPrice(ctx context.Context, scope domain.Scope, revision string) (*domain.PriceRecord, error) {
	if err := financeReader(scope, ""); err != nil {
		return nil, err
	}
	var rec *domain.PriceRecord
	err := f.store.Read(ctx, func(r Repo) error {
		var err error
		rec, err = r.GetPriceRevision(ctx, revision)
		return err
	})
	return rec, err
}

// PoolSpec is the scope, period and cap of a pool to fund.
type PoolSpec struct {
	Level       domain.BudgetLevel
	TenantID    string
	ActorID     string
	PeriodStart time.Time
	PeriodEnd   time.Time
	Cap         domain.Money
}

func (s PoolSpec) validate() error {
	switch s.Level {
	case domain.LevelPlatform:
		if s.TenantID != "" || s.ActorID != "" {
			return fmt.Errorf("%w: a platform pool names no tenant or actor", domain.ErrInvalid)
		}
	case domain.LevelTenant:
		if s.TenantID == "" || s.ActorID != "" {
			return fmt.Errorf("%w: a tenant pool names its tenant and no actor", domain.ErrInvalid)
		}
	case domain.LevelActor:
		if s.TenantID == "" || s.ActorID == "" {
			return fmt.Errorf("%w: an actor pool names its tenant and actor", domain.ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: pool level %q", domain.ErrInvalid, s.Level)
	}
	if !s.PeriodEnd.After(s.PeriodStart) {
		return fmt.Errorf("%w: pool period is empty", domain.ErrInvalid)
	}
	if s.Cap.Amount < 0 {
		return fmt.Errorf("%w: pool cap is negative", domain.ErrInvalid)
	}
	return nil
}

// CreatePool funds a new pool. The same scope and period with the same
// currency, end and cap returns the existing pool; anything else under the
// same scope and period is an idempotency conflict.
func (f *Finance) CreatePool(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, spec PoolSpec) (pool *domain.BudgetPool, existing bool, err error) {
	if err := financeWriter(scope, cmd, ActionFundPool); err != nil {
		return nil, false, err
	}
	spec.PeriodStart, spec.PeriodEnd = spec.PeriodStart.UTC().Truncate(time.Microsecond), spec.PeriodEnd.UTC().Truncate(time.Microsecond)
	if err := spec.validate(); err != nil {
		return nil, false, err
	}
	err = f.store.Tx(ctx, func(r Repo) error {
		found, err := r.GetPoolByScope(ctx, spec.Level, spec.TenantID, spec.ActorID, spec.PeriodStart)
		switch {
		case err == nil:
			if found.Currency != spec.Cap.Currency || found.Cap != spec.Cap.Amount || !found.PeriodEnd.Equal(spec.PeriodEnd) {
				return fmt.Errorf("%w: pool %s already funds this scope and period with other terms", domain.ErrIdempotencyConflict, found.ID)
			}
			pool, existing = found, true
			return nil
		case !errors.Is(err, domain.ErrNotFound):
			return err
		}
		pool = &domain.BudgetPool{ID: domain.NewID("pool"), Level: spec.Level, TenantID: spec.TenantID, ActorID: spec.ActorID, PeriodStart: spec.PeriodStart, PeriodEnd: spec.PeriodEnd, Currency: spec.Cap.Currency, Cap: spec.Cap.Amount, Revision: 1}
		return r.InsertPool(ctx, pool, cmd.ActorID)
	})
	if errors.Is(err, ErrDuplicateKey) && depthOf(ctx) < duplicateRetries {
		return f.CreatePool(deeper(ctx), cmd, scope, spec)
	}
	if err == nil && !existing {
		f.log.Info("budget pool funded", "poolId", pool.ID, "level", pool.Level, "tenantId", pool.TenantID, "cap", spec.Cap.String(), "actor", cmd.ActorID)
	}
	return pool, existing, err
}

// AdjustPool sets a pool's cap conditionally on its revision; a cap below
// the allocated amount fences new allocations and returns nothing.
func (f *Finance) AdjustPool(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, poolID string, expected uint64, capAmount domain.Money) (*domain.BudgetPool, error) {
	if err := financeWriter(scope, cmd, ActionFundPool); err != nil {
		return nil, err
	}
	if capAmount.Amount < 0 {
		return nil, fmt.Errorf("%w: pool cap is negative", domain.ErrInvalid)
	}
	var pool *domain.BudgetPool
	err := f.store.Tx(ctx, func(r Repo) error {
		locked, err := r.LockPool(ctx, poolID)
		if err != nil {
			return err
		}
		if locked.Revision != expected {
			return fmt.Errorf("%w: pool %s is at revision %d, not %d", domain.ErrRevisionConflict, poolID, locked.Revision, expected)
		}
		if locked.Currency != capAmount.Currency {
			return fmt.Errorf("%w: pool %s is in %s", domain.ErrInvalid, poolID, locked.Currency)
		}
		locked.Cap = capAmount.Amount
		locked.Revision++
		pool = locked
		return r.UpdatePoolCap(ctx, locked)
	})
	if err == nil {
		f.log.Info("budget pool adjusted", "poolId", poolID, "cap", capAmount.String(), "revision", pool.Revision, "actor", cmd.ActorID)
	}
	return pool, err
}

func (f *Finance) GetPool(ctx context.Context, scope domain.Scope, poolID string) (*domain.BudgetPool, error) {
	var pool *domain.BudgetPool
	err := f.store.Read(ctx, func(r Repo) error {
		var err error
		pool, err = r.GetPool(ctx, poolID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := financeReader(scope, pool.TenantID); err != nil {
		return nil, err
	}
	return pool, nil
}

// SetCostCap creates or changes the cap of a scope; with expected set, the
// change applies only at that revision (a cap that does not exist yet has
// no revision). Lowering a cap fences the next admission only.
func (f *Finance) SetCostCap(ctx context.Context, cmd domain.CommandIdentity, scope domain.Scope, level domain.CapLevel, tenantID, projectID string, capAmount domain.Money, expected *uint64) (*domain.CostCap, error) {
	if err := financeWriter(scope, cmd, ActionSetCostCap); err != nil {
		return nil, err
	}
	switch {
	case level != domain.CapTenant && level != domain.CapProject && level != domain.CapOperation:
		return nil, fmt.Errorf("%w: cost cap level %q", domain.ErrInvalid, level)
	case tenantID == "":
		return nil, fmt.Errorf("%w: a cost cap names its tenant", domain.ErrInvalid)
	case level == domain.CapTenant && projectID != "":
		return nil, fmt.Errorf("%w: a tenant cost cap names no project", domain.ErrInvalid)
	case capAmount.Amount < 0:
		return nil, fmt.Errorf("%w: cost cap is negative", domain.ErrInvalid)
	}
	now := f.clock.Now().UTC().Truncate(time.Microsecond)
	var out *domain.CostCap
	err := f.store.Tx(ctx, func(r Repo) error {
		found, err := r.GetCostCap(ctx, level, tenantID, projectID, capAmount.Currency)
		if errors.Is(err, domain.ErrNotFound) {
			if expected != nil {
				return fmt.Errorf("%w: no %s cost cap exists yet", domain.ErrRevisionConflict, level)
			}
			out = &domain.CostCap{ID: domain.NewID("cap"), Level: level, TenantID: tenantID, ProjectID: projectID, Currency: capAmount.Currency, Cap: capAmount.Amount, Revision: 1, UpdatedBy: cmd.ActorID, UpdatedAt: now}
			return r.InsertCostCap(ctx, out)
		}
		if err != nil {
			return err
		}
		locked, err := r.LockCostCap(ctx, found.ID)
		if err != nil {
			return err
		}
		if expected != nil && locked.Revision != *expected {
			return fmt.Errorf("%w: cost cap %s is at revision %d, not %d", domain.ErrRevisionConflict, locked.ID, locked.Revision, *expected)
		}
		locked.Cap, locked.UpdatedBy, locked.UpdatedAt = capAmount.Amount, cmd.ActorID, now
		locked.Revision++
		out = locked
		return r.UpdateCostCapLimit(ctx, locked)
	})
	if errors.Is(err, ErrDuplicateKey) && depthOf(ctx) < duplicateRetries {
		return f.SetCostCap(deeper(ctx), cmd, scope, level, tenantID, projectID, capAmount, expected)
	}
	if err == nil {
		f.log.Info("cost cap set", "capId", out.ID, "level", level, "tenantId", tenantID, "projectId", projectID, "cap", capAmount.String(), "revision", out.Revision, "actor", cmd.ActorID)
	}
	return out, err
}

func (f *Finance) GetCostCap(ctx context.Context, scope domain.Scope, level domain.CapLevel, tenantID, projectID, currency string) (*domain.CostCap, error) {
	if err := financeReader(scope, tenantID); err != nil {
		return nil, err
	}
	var out *domain.CostCap
	err := f.store.Read(ctx, func(r Repo) error {
		var err error
		out, err = r.GetCostCap(ctx, level, tenantID, projectID, currency)
		return err
	})
	return out, err
}

// SettleFinance settles a terminal operation's money once nothing can
// change it any more (P2.1, CTL-APP-09): no dispatch holds a reservation or
// an unknown outcome. The unspent part of every allocation returns to its
// pool under the pool locks (rank 1, in id order, before the allocations
// and the operation), the return ledger records it, and finance becomes
// settled with an event. An operation that was never funded settles with
// nothing to return. It reports whether this call settled the operation.
func (f *Finance) SettleFinance(ctx context.Context, operationID string) (op *domain.Operation, settled bool, err error) {
	now := f.clock.Now()
	err = f.store.Tx(ctx, func(r Repo) error {
		peek, err := r.GetOperation(ctx, operationID)
		if err != nil {
			return err
		}
		if !financeSettleable(peek) {
			op = peek
			return nil
		}
		allocs, err := r.ListAllocationsByOperation(ctx, operationID)
		if err != nil {
			return err
		}
		poolIDs := make([]string, 0, len(allocs))
		for _, a := range allocs {
			poolIDs = append(poolIDs, a.PoolID)
		}
		sort.Strings(poolIDs)
		pools := map[string]*domain.BudgetPool{}
		for _, id := range poolIDs {
			p, err := r.LockPool(ctx, id)
			if err != nil {
				return err
			}
			pools[id] = p
		}
		locked, err := r.LockAllocations(ctx, operationID)
		if err != nil {
			return err
		}
		lockedOp, err := r.LockOperation(ctx, operationID)
		if err != nil {
			return err
		}
		op = lockedOp
		if !financeSettleable(lockedOp) {
			return nil
		}
		unsettled, err := r.HasUnsettledDispatch(ctx, operationID)
		if err != nil || unsettled {
			return err
		}
		for _, a := range locked {
			if a.Reserved != 0 {
				return nil // a reservation is still held: not settleable yet
			}
		}
		for _, a := range locked {
			returned, err := r.HasAllocationReturn(ctx, a.ID)
			if err != nil {
				return err
			}
			if returned {
				continue
			}
			unspent, err := a.ReturnUnspent()
			if err != nil {
				return err
			}
			pool := pools[a.PoolID]
			if pool == nil {
				return fmt.Errorf("%w: allocation %s names pool %s that was not locked", domain.ErrIntegrity, a.ID, a.PoolID)
			}
			if err := pool.Return(unspent); err != nil {
				return err
			}
			if err := r.UpdatePool(ctx, pool); err != nil {
				return err
			}
			if err := r.InsertAllocationReturn(ctx, a, unspent, now); err != nil {
				return err
			}
		}
		ev := lockedOp.Transition("finance:settled", now, func(o *domain.Operation) { o.Finance = domain.FinanceSettled })
		if err := r.UpdateOperation(ctx, lockedOp); err != nil {
			return err
		}
		settled = true
		return r.InsertEvent(ctx, ev)
	})
	return op, settled, err
}

func financeSettleable(op *domain.Operation) bool {
	return op.Lifecycle.Terminal() && (op.Finance == domain.FinanceFunded || op.Finance == domain.FinanceNotFunded)
}

// ReconcileFinance settles up to limit terminal operations whose finance
// has not settled yet (the sweep behind the inline settlement).
func (f *Finance) ReconcileFinance(ctx context.Context, limit int) (int, error) {
	var ids []string
	if err := f.store.Read(ctx, func(r Repo) error {
		var err error
		ids, err = r.ListFinanceUnsettled(ctx, limit)
		return err
	}); err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
		_, settled, err := f.SettleFinance(ctx, id)
		if err != nil {
			f.log.Warn("financial settlement failed", "operationId", id, "error", err)
			continue
		}
		if settled {
			n++
		}
	}
	return n, nil
}
