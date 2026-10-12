package application_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	mrand "math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/authority"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/evidence"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// productionChain is Control's dispatch chain with every production
// authority input (P2.1) and no development fixture: the database price
// book, a declared decision endpoint (a fake ENV-07 source), signed
// evidence and required cost caps.
type productionChain struct {
	pool      *pgxpool.Pool
	inv       *inventory.Filesystem
	prices    *application.StoredPrices
	finance   *application.Finance
	ops       *application.Operations
	exec      *application.Execution
	dispatch  *application.Dispatch
	key       ed25519.PrivateKey
	sourceUp  atomic.Bool
	decisions atomic.Int32
	start     time.Time
	platform  *domain.BudgetPool
}

// platformCap funds the one platform pool of a chain.
const platformCap = int64(1_000_000_000_000)

var (
	platformOperator = domain.Scope{TenantID: "tenant_ops", ActorID: "user_tenant_ops", Roles: []string{domain.RolePlatformOperator}}
	financeTenant    = domain.Scope{TenantID: "tenant_f", ProjectID: "proj_f", ActorID: "user_f"}
)

func opsCmd(id string) domain.CommandIdentity { return cmd("tenant_ops", id, id) }

func newProductionChain(t *testing.T, inst *testdb.Instance) *productionChain {
	t.Helper()
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	c := &productionChain{pool: pool, inv: inv}
	c.sourceUp.Store(true)
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.decisions.Add(1)
		if !c.sourceUp.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintf(w, `{"decision":"allow","revision":"pagix-auth-1","freshUntil":%q}`, time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano))
	}))
	t.Cleanup(src.Close)
	auth, err := authority.New(authority.Options{URL: src.URL, Freshness: time.Second, Timeout: time.Second}, domain.SystemClock{})
	require.NoError(t, err)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	c.key = priv
	der, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)
	keyPath := filepath.Join(t.TempDir(), "issuer.pem")
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0o600))
	issuers, err := evidence.LoadIssuers([]evidence.IssuerSpec{{Name: "integration-supervisor", KeyID: "sup-1", PublicKeyFile: keyPath}})
	require.NoError(t, err)
	c.prices = application.NewStoredPrices(store, testLog)
	c.finance = application.NewFinance(store, c.prices.Refresh, domain.SystemClock{}, testLog)
	c.ops = application.NewOperations(store, inv, []domain.Profile{profile}, domain.SystemClock{}, testLog)
	c.exec = application.NewExecution(store, inv, manifests, []domain.Profile{profile}, domain.SystemClock{}, testLog).WithFinance(c.finance)
	c.dispatch = application.NewDispatch(store, inv, c.prices, auth, evidence.NewVerifier(inv, issuers), domain.SystemClock{}, testLog)
	c.start = time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	c.platform, _, err = c.finance.CreatePool(context.Background(), opsCmd("pool_platform"), platformOperator, application.PoolSpec{Level: domain.LevelPlatform, PeriodStart: c.start, PeriodEnd: c.start.Add(48 * time.Hour), Cap: usd(platformCap)})
	require.NoError(t, err)
	return c
}

func (c *productionChain) publish(t *testing.T, revision string, perMillion int64, from time.Time) {
	t.Helper()
	_, _, err := c.finance.PublishPrice(context.Background(), opsCmd("publish_"+revision), platformOperator, domain.Price{
		Revision: revision, Kind: domain.DispatchModel, Route: modelRoute, Provider: modelProvider, Model: modelName, Currency: "USD",
		EffectiveFrom: from, EffectiveUntil: from.Add(24 * time.Hour), MaxExposure: 1_000_000,
		PerMillion: map[domain.UsageCategory]int64{domain.UsageInput: perMillion, domain.UsageOutput: perMillion, domain.UsageReasoning: perMillion, domain.UsageCachedInput: perMillion},
	})
	require.NoError(t, err)
}

// fund creates the tenant and actor pools of the scope through the
// operator use case; the chain's platform pool is shared.
func (c *productionChain) fund(t *testing.T, scope domain.Scope, capAmount int64) []*domain.BudgetPool {
	t.Helper()
	start, end := c.start, c.start.Add(48*time.Hour)
	var out []*domain.BudgetPool
	for _, spec := range []application.PoolSpec{
		{Level: domain.LevelTenant, TenantID: scope.TenantID},
		{Level: domain.LevelActor, TenantID: scope.TenantID, ActorID: scope.ActorID},
	} {
		spec.PeriodStart, spec.PeriodEnd, spec.Cap = start, end, usd(capAmount)
		p, _, err := c.finance.CreatePool(context.Background(), opsCmd(fmt.Sprintf("pool_%s_%s_%d", spec.Level, scope.TenantID, start.UnixNano())), platformOperator, spec)
		require.NoError(t, err)
		out = append(out, p)
	}
	return out
}

func (c *productionChain) caps(t *testing.T, scope domain.Scope, tenant, project, operation int64) {
	t.Helper()
	for _, l := range []struct {
		level   domain.CapLevel
		project string
		amount  int64
	}{{domain.CapTenant, "", tenant}, {domain.CapProject, scope.ProjectID, project}, {domain.CapOperation, "", operation}} {
		_, err := c.finance.SetCostCap(context.Background(), opsCmd(fmt.Sprintf("cap_%s_%s_%d", l.level, scope.TenantID, l.amount)), platformOperator, l.level, scope.TenantID, l.project, usd(l.amount), nil)
		require.NoError(t, err)
	}
}

func (c *productionChain) operation(t *testing.T, scope domain.Scope, id string, funding int64) (*domain.Operation, *domain.Attempt) {
	t.Helper()
	ctx := context.Background()
	op, _, err := c.ops.Create(ctx, cmd(scope.TenantID, "cmd_"+id, id), scope, domain.KindLocalCheck, subject, nil)
	require.NoError(t, err)
	at, _, err := c.exec.OpenAttempt(ctx, cmd(scope.TenantID, "cmd_"+id+"_open", "open"), op.ID, "local-check", 0, "local-check-v1")
	require.NoError(t, err)
	if funding > 0 {
		_, _, err = c.dispatch.Allocate(ctx, scope, op.ID, usd(funding))
		require.NoError(t, err)
	}
	return op, at
}

func (c *productionChain) admit(t *testing.T, scope domain.Scope, op *domain.Operation, at *domain.Attempt, callID string, exposure int64) (application.Admission, error) {
	t.Helper()
	req := modelRequest(op, at, callID, "anvilkit-agent-model-proxy", exposure)
	return c.dispatch.Admit(context.Background(), cmd(scope.TenantID, "admit_"+callID, callID), req)
}

// TestProductionAdmission: with no development fixture, a published price
// revision, operator-funded pools and caps admit a paid model call; the
// dispatch freezes the revision and a later publication does not change
// what the in-flight call costs (P2.1 AC1). An authority outage refuses
// without recording the call (AC3).
func TestProductionAdmission(t *testing.T) {
	inst := testdb.Start(t)
	c := newProductionChain(t, inst)
	ctx := context.Background()
	now := time.Now().UTC()

	_, _, err := c.finance.PublishPrice(ctx, cmd("tenant_f", "publish_x", "x"), financeTenant, domain.Price{Revision: "x"})
	require.ErrorIs(t, err, domain.ErrForbidden, "only a platform operator publishes prices")

	c.publish(t, "fx-prod-1", 1_000_000, now.Add(-time.Hour))
	_, existing, err := c.finance.PublishPrice(ctx, opsCmd("publish_again"), platformOperator, domain.Price{
		Revision: "fx-prod-1", Kind: domain.DispatchModel, Route: modelRoute, Provider: modelProvider, Model: modelName, Currency: "USD",
		EffectiveFrom: now.Add(-time.Hour), EffectiveUntil: now.Add(23 * time.Hour), MaxExposure: 1_000_000,
		PerMillion: map[domain.UsageCategory]int64{domain.UsageInput: 2, domain.UsageOutput: 2, domain.UsageReasoning: 2, domain.UsageCachedInput: 2},
	})
	require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "a revision never changes its content")
	require.False(t, existing)
	c.fund(t, financeTenant, 100_000_000)
	c.caps(t, financeTenant, 50_000_000, 50_000_000, 10_000_000)
	op, at := c.operation(t, financeTenant, "prod_1", 5_000_000)

	a, err := c.admit(t, financeTenant, op, at, "call_prod_1", 500_000)
	require.NoError(t, err)
	require.True(t, a.Allowed, "the production chain admits the paid route: %s", a.DenialCode)
	require.Equal(t, "fx-prod-1", a.Dispatch.MeterRevision, "the revision is frozen at admission")

	// A later, dearer revision of the route: new admissions use it, the
	// in-flight call keeps the frozen one.
	c.publish(t, "fx-prod-2", 9_000_000, now.Add(-time.Minute))
	d, _, err := c.dispatch.Observe(ctx, application.Reader{}, a.Dispatch.ID, "proxy", 1, domain.DispatchSucceeded, reported(domain.Usage{Input: 100_000, Output: 100_000}), "", time.Now())
	require.NoError(t, err)
	require.Equal(t, domain.DispatchObserved, d.State)
	_, total := entries(t, c.pool, d.ID)
	require.Equal(t, int64(200_000), total, "priced under the frozen revision (1 per unit), not the later one (9 per unit)")
	b, err := c.admit(t, financeTenant, op, at, "call_prod_2", 500_000)
	require.NoError(t, err)
	require.True(t, b.Allowed)
	require.Equal(t, "fx-prod-2", b.Dispatch.MeterRevision)

	// Withdrawing the revision of a prepared call keeps it metering.
	_, _, err = c.finance.WithdrawPrice(ctx, opsCmd("withdraw_2"), platformOperator, "fx-prod-2", "superseded")
	require.NoError(t, err)
	d2, _, err := c.dispatch.Observe(ctx, application.Reader{}, b.Dispatch.ID, "proxy", 1, domain.DispatchSucceeded, reported(domain.Usage{Input: 10_000}), "", time.Now())
	require.NoError(t, err, "the withdrawn revision still meters the call admitted under it")
	_, total2 := entries(t, c.pool, d2.ID)
	require.Equal(t, int64(90_000), total2)
	again, err := c.admit(t, financeTenant, op, at, "call_prod_3", 100_000)
	require.NoError(t, err)
	require.Equal(t, "fx-prod-1", again.Dispatch.MeterRevision, "new admission skips the withdrawn revision")

	// AC3: the authority source down → refused, nothing recorded, and the
	// same call is admitted once the source answers again.
	c.sourceUp.Store(false)
	time.Sleep(1100 * time.Millisecond) // past Control's 1 s freshness bound of the test
	_, err = c.admit(t, financeTenant, op, at, "call_outage", 100_000)
	require.ErrorIs(t, err, domain.ErrUnavailable)
	require.ErrorContains(t, err, "AUTHORITY_UNAVAILABLE")
	var rows int
	require.NoError(t, c.pool.QueryRow(ctx, "SELECT count(*) FROM dispatches WHERE call_id = 'call_outage'").Scan(&rows))
	require.Zero(t, rows, "an outage never burns the call identity")
	c.sourceUp.Store(true)
	ok, err := c.admit(t, financeTenant, op, at, "call_outage", 100_000)
	require.NoError(t, err)
	require.True(t, ok.Allowed)
}

// TestCostCaps: admission is denied CAP_EXCEEDED at each level
// independently; a cap lowered while a call is in flight does not revoke
// that call and fences the next one; a missing cap denies the paid route
// (P2.1 AC2).
func TestCostCaps(t *testing.T) {
	inst := testdb.Start(t)
	c := newProductionChain(t, inst)
	ctx := context.Background()
	c.publish(t, "fx-caps-1", 1_000_000, time.Now().Add(-time.Hour))

	missing := domain.Scope{TenantID: "tenant_nocap", ProjectID: "p", ActorID: "u"}
	c.fund(t, missing, 10_000_000)
	op, at := c.operation(t, missing, "nocap", 1_000_000)
	a, err := c.admit(t, missing, op, at, "call_nocap", 100_000)
	require.NoError(t, err)
	require.False(t, a.Allowed)
	require.Equal(t, domain.DenyCapExceeded, a.DenialCode, "a missing cap denies the paid route")

	for _, level := range []domain.CapLevel{domain.CapTenant, domain.CapProject, domain.CapOperation} {
		t.Run(string(level), func(t *testing.T) {
			s := domain.Scope{TenantID: "tenant_" + string(level), ProjectID: "proj_" + string(level), ActorID: "user"}
			c.fund(t, s, 100_000_000)
			tenant, project, operation := int64(10_000_000), int64(10_000_000), int64(10_000_000)
			switch level {
			case domain.CapTenant:
				tenant = 150_000
			case domain.CapProject:
				project = 150_000
			case domain.CapOperation:
				operation = 150_000
			}
			c.caps(t, s, tenant, project, operation)
			op, at := c.operation(t, s, "cap_"+string(level), 5_000_000)
			first, err := c.admit(t, s, op, at, "call_"+string(level)+"_1", 100_000)
			require.NoError(t, err)
			require.True(t, first.Allowed)
			second, err := c.admit(t, s, op, at, "call_"+string(level)+"_2", 100_000)
			require.NoError(t, err)
			require.False(t, second.Allowed, "the %s cap holds 100000 reserved; 100000 more exceeds 150000", level)
			require.Equal(t, domain.DenyCapExceeded, second.DenialCode)
		})
	}

	t.Run("lowered while in flight", func(t *testing.T) {
		s := domain.Scope{TenantID: "tenant_lowered", ProjectID: "proj_l", ActorID: "user"}
		c.fund(t, s, 100_000_000)
		c.caps(t, s, 10_000_000, 10_000_000, 10_000_000)
		op, at := c.operation(t, s, "lowered", 5_000_000)
		inflight, err := c.admit(t, s, op, at, "call_lowered_1", 200_000)
		require.NoError(t, err)
		require.True(t, inflight.Allowed)
		capRow, err := c.finance.GetCostCap(ctx, platformOperator, domain.CapTenant, s.TenantID, "", "USD")
		require.NoError(t, err)
		require.Equal(t, int64(200_000), capRow.Reserved, "the tenant cap counts the reservation")
		rev := capRow.Revision
		_, err = c.finance.SetCostCap(ctx, opsCmd("lower_tenant"), platformOperator, domain.CapTenant, s.TenantID, "", usd(100_000), &rev)
		require.NoError(t, err)
		d, _, err := c.dispatch.Observe(ctx, application.Reader{}, inflight.Dispatch.ID, "proxy", 1, domain.DispatchSucceeded, reported(domain.Usage{Input: 150_000}), "", time.Now())
		require.NoError(t, err, "the authorized call is not revoked by the lower cap")
		require.Equal(t, domain.DispatchObserved, d.State)
		capRow, err = c.finance.GetCostCap(ctx, platformOperator, domain.CapTenant, s.TenantID, "", "USD")
		require.NoError(t, err)
		require.Equal(t, int64(0), capRow.Reserved)
		require.Equal(t, int64(150_000), capRow.Consumed, "the incurred cost stays above the lowered cap")
		next, err := c.admit(t, s, op, at, "call_lowered_2", 1)
		require.NoError(t, err)
		require.False(t, next.Allowed)
		require.Equal(t, domain.DenyCapExceeded, next.DenialCode, "the lowered cap fences the next send")
	})
}

// TestSignedNotSentEvidence: unsigned or wrongly signed attestations are
// refused; a signed one from the configured issuer confirms not_sent and
// releases the exposure of the allocations and the caps (P2.1 AC4).
func TestSignedNotSentEvidence(t *testing.T) {
	inst := testdb.Start(t)
	c := newProductionChain(t, inst)
	ctx := context.Background()
	c.publish(t, "fx-ev-1", 1_000_000, time.Now().Add(-time.Hour))
	c.fund(t, financeTenant, 100_000_000)
	c.caps(t, financeTenant, 50_000_000, 50_000_000, 10_000_000)
	op, at := c.operation(t, financeTenant, "evidence", 5_000_000)
	a, err := c.admit(t, financeTenant, op, at, "call_ev", 300_000)
	require.NoError(t, err)
	require.True(t, a.Allowed)
	d := a.Dispatch
	payload := evidence.NotSent{Class: "not-sent", TenantID: d.TenantID, DispatchID: d.ID, CallID: d.CallID, Owner: d.Owner, ExecutionEpoch: fmt.Sprint(d.ExecutionEpoch), Issuer: "integration-supervisor", SealedAt: time.Now().UTC().Format(time.RFC3339)}
	ref := evidence.NotSentKey(d.ID)
	publish := func(body []byte) domain.Digest {
		// Each attempt publishes under its own inventory root: the
		// inventory never overwrites an object.
		path := filepath.Join(c.inv.Root(), ref)
		_ = os.Remove(path)
		_, err := c.inv.Put(ctx, ref, body)
		require.NoError(t, err)
		return application.DigestOf(body)
	}
	unsigned := []byte(fmt.Sprintf(`{"class":"not-sent","dispatchId":%q,"callId":%q,"owner":%q,"issuer":"integration-supervisor","sealedAt":%q}`, d.ID, d.CallID, d.Owner, payload.SealedAt))
	_, _, err = c.dispatch.ConfirmNotSent(ctx, cmd(d.TenantID, "cns_1", "1"), d.Owner, d.ID, ref, publish(unsigned))
	require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "unsigned JSON is not evidence outside development")
	_, foreign, _ := ed25519.GenerateKey(rand.Reader)
	wrong, err := evidence.Sign(foreign, "sup-1", payload)
	require.NoError(t, err)
	_, _, err = c.dispatch.ConfirmNotSent(ctx, cmd(d.TenantID, "cns_2", "2"), d.Owner, d.ID, ref, publish(wrong))
	require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "a signature by another key is refused")
	good, err := evidence.Sign(c.key, "sup-1", payload)
	require.NoError(t, err)
	confirmed, _, err := c.dispatch.ConfirmNotSent(ctx, cmd(d.TenantID, "cns_3", "3"), d.Owner, d.ID, ref, publish(good))
	require.NoError(t, err)
	require.Equal(t, domain.DispatchConfirmedNotSent, confirmed.State)
	reserved, _ := allocations(t, c.pool, op.ID)
	require.Equal(t, []int64{0, 0, 0}, reserved, "the exposure is released")
	capRow, err := c.finance.GetCostCap(ctx, platformOperator, domain.CapTenant, financeTenant.TenantID, "", "USD")
	require.NoError(t, err)
	require.Zero(t, capRow.Reserved)
}

// TestFinancialSettlementProperty: across 100 randomized operations every
// terminal operation settles, returns its unspent allocation, and every
// pool's allocated amount equals the actual cost of its settled operations
// (available = funded − actual cost) (P2.1 AC6).
func TestFinancialSettlementProperty(t *testing.T) {
	inst := testdb.Start(t)
	c := newProductionChain(t, inst)
	ctx := context.Background()
	c.publish(t, "fx-settle-1", 1_000_000, time.Now().Add(-time.Hour))
	s := domain.Scope{TenantID: "tenant_settle", ProjectID: "proj_s", ActorID: "user_s"}
	const funded = int64(10_000_000_000)
	pools := c.fund(t, s, funded)
	require.Len(t, pools, 2)
	c.caps(t, s, funded, funded, funded)
	rng := mrand.New(mrand.NewPCG(20261011, 2))
	var actual int64
	for i := 0; i < 100; i++ {
		amount := int64(500_000 + rng.IntN(5_000_000))
		op, at := c.operation(t, s, fmt.Sprintf("settle_%03d", i), amount)
		for j := 0; j < rng.IntN(4); j++ {
			exposure := int64(1 + rng.IntN(int(amount/4)))
			a, err := c.admit(t, s, op, at, fmt.Sprintf("call_s_%03d_%d", i, j), exposure)
			require.NoError(t, err)
			if !a.Allowed {
				continue
			}
			switch rng.IntN(3) {
			case 0: // a send with usage (bounded by its exposure at 1 per unit)
				units := uint64(rng.IntN(int(exposure) + 1))
				_, _, err = c.dispatch.Observe(ctx, application.Reader{}, a.Dispatch.ID, "proxy", 1, domain.DispatchSucceeded, reported(domain.Usage{Input: units}), "", time.Now())
				require.NoError(t, err)
				actual += int64(units)
			case 1: // a failed send that metered nothing
				_, _, err = c.dispatch.Observe(ctx, application.Reader{}, a.Dispatch.ID, "proxy", 1, domain.DispatchFailed, reported(domain.Usage{}), "", time.Now())
				require.NoError(t, err)
			default: // left prepared-and-authorized, then closed: settled by a failed observation
				_, _, err = c.dispatch.Observe(ctx, application.Reader{}, a.Dispatch.ID, "proxy", 1, domain.DispatchCanceled, reported(domain.Usage{}), "", time.Now())
				require.NoError(t, err)
			}
		}
		_, closed, _, err := c.exec.CloseAttempt(ctx, cmd(s.TenantID, fmt.Sprintf("close_s_%03d", i), "c"), at.ID, domain.OutcomeInfrastructureFailed, domain.CleanupNotRequired, "OBSERVER_FAILED")
		require.NoError(t, err)
		require.True(t, closed.Lifecycle.Terminal())
		require.Equal(t, domain.FinanceSettled, closed.Finance, "operation %d settles at once", i)
	}
	var unsettled int
	require.NoError(t, c.pool.QueryRow(ctx, "SELECT count(*) FROM operations WHERE tenant_id = $1 AND finance_state <> 'settled'", s.TenantID).Scan(&unsettled))
	require.Zero(t, unsettled)
	for _, p := range append(pools, c.platform) {
		var allocated, capAmount, returned, amounts int64
		require.NoError(t, c.pool.QueryRow(ctx, "SELECT allocated, cap_amount FROM budget_pools WHERE pool_id = $1", p.ID).Scan(&allocated, &capAmount))
		require.NoError(t, c.pool.QueryRow(ctx, "SELECT coalesce(sum(amount), 0) FROM allocation_returns WHERE pool_id = $1", p.ID).Scan(&returned))
		require.NoError(t, c.pool.QueryRow(ctx, "SELECT coalesce(sum(amount), 0) FROM allocations WHERE pool_id = $1", p.ID).Scan(&amounts))
		require.Equal(t, actual, allocated, "%s pool: allocated equals the actual cost of the settled operations", p.Level)
		require.Equal(t, amounts-returned, allocated, "%s pool: allocated = Σ allocations − Σ returns", p.Level)
		pool, err := c.finance.GetPool(ctx, platformOperator, p.ID)
		require.NoError(t, err)
		require.Equal(t, capAmount-actual, pool.Cap-pool.Allocated, "%s pool: available = funded − actual cost", p.Level)
	}
	// A repeated settlement is a no-op.
	n, err := c.finance.ReconcileFinance(ctx, 100)
	require.NoError(t, err)
	require.Zero(t, n)
}
