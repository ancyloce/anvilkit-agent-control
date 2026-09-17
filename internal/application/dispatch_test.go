package application_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/development"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

const (
	modelRoute    = "fixture-route"
	modelProvider = "fixture"
	modelName     = "fixture-model"
	toolRoute     = "srv/read"
	issuer        = "dev-supervisor"
)

var usd = func(amount int64) domain.Money { return domain.Money{Currency: "USD", Amount: amount} }

// fixturePrices are DEVELOPMENT_ONLY observations for one model route
// (bound to its trusted provider/model) and one tool route (per-million
// unit prices at scale 6). Every unit price is multiplied by factor so a
// second observation of the same route can be told apart in the ledger.
func fixturePrices(t *testing.T, revision string, factor int64) application.PriceBook {
	t.Helper()
	perMillion := map[domain.UsageCategory]int64{domain.UsageInput: 3_000_000 * factor, domain.UsageOutput: 15_000_000 * factor, domain.UsageReasoning: 15_000_000 * factor, domain.UsageCachedInput: 300_000 * factor}
	from, until := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	book, err := development.NewPriceBook([]domain.Price{
		{Revision: revision, Kind: domain.DispatchModel, Route: modelRoute, Provider: modelProvider, Model: modelName, Currency: "USD", EffectiveFrom: from, EffectiveUntil: until, PerMillion: perMillion, MaxExposure: 1_000_000},
		{Revision: "fx-tool-1", Kind: domain.DispatchTool, Route: toolRoute, Currency: "USD", EffectiveFrom: from, EffectiveUntil: until, PerMillion: perMillion, MaxExposure: 1_000_000},
	})
	require.NoError(t, err)
	return book
}

// priceSource is the price port with a replaceable current book: the
// observation a route resolves to can change after a call was admitted,
// while every revision ever published stays retrievable, as a real price
// source (ENV-06) would behave across a price change.
type priceSource struct {
	mu        sync.Mutex
	current   application.PriceBook
	published []application.PriceBook
}

func newPriceSource(book application.PriceBook) *priceSource {
	return &priceSource{current: book, published: []application.PriceBook{book}}
}

func (p *priceSource) publish(book application.PriceBook) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = book
	p.published = append(p.published, book)
}

func (p *priceSource) Price(kind domain.DispatchKind, route string, at time.Time) (*domain.Price, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current.Price(kind, route, at)
}

func (p *priceSource) PriceByRevision(revision string) (*domain.Price, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, b := range p.published {
		if price, ok := b.PriceByRevision(revision); ok {
			return price, true
		}
	}
	return nil, false
}

// authorityDouble is the current-authority port with switches for the race
// scenarios: a decision can be denied or already expired when it is read.
type authorityDouble struct {
	allowed map[string]bool
	denied  atomic.Bool
	expired atomic.Bool
	reads   atomic.Int32
}

func (a *authorityDouble) CheckExecution(_ context.Context, scope domain.Scope, routeID string) (domain.Decision, error) {
	a.reads.Add(1)
	now := time.Now()
	fresh := now.Add(30 * time.Second)
	if a.expired.Load() {
		fresh = now.Add(-time.Second)
	}
	if a.denied.Load() || !a.allowed[scope.TenantID+"/"+routeID] {
		return domain.Decision{Allow: false, Revision: "double", ReasonCode: "ROUTE_NOT_AUTHORIZED", FreshUntil: fresh}, nil
	}
	return domain.Decision{Allow: true, Revision: "double", FreshUntil: fresh}, nil
}

// CheckOperator allows the fixture operator of the recovery scenarios.
func (a *authorityDouble) CheckOperator(_ context.Context, scope domain.Scope, action string) (domain.Decision, error) {
	now := time.Now()
	if scope.ActorID == operatorActor {
		return domain.Decision{Allow: true, Revision: "double", FreshUntil: now.Add(30 * time.Second)}, nil
	}
	return domain.Decision{Allow: false, Revision: "double", ReasonCode: "OPERATOR_NOT_AUTHORIZED:" + action, FreshUntil: now.Add(30 * time.Second)}, nil
}

const operatorActor = "operator_a"

// hookedInventory is the filesystem inventory with the flaky behaviors of
// the intake tests plus a hook that runs between the two admission
// transactions (the moment authority can change) and a lock probe that
// proves no row lock is held while the object is written.
type hookedInventory struct {
	inner   *inventory.Filesystem
	failing atomic.Bool
	ackLost atomic.Bool
	mu      sync.Mutex
	onPut   func(key string)
	probe   *pgxpool.Pool
	locked  atomic.Int32
	puts    atomic.Int32
}

func (h *hookedInventory) Put(ctx context.Context, key string, body []byte) (string, error) {
	h.puts.Add(1)
	if h.probe != nil {
		h.assertNoLocksHeld(ctx, key)
	}
	h.mu.Lock()
	hook := h.onPut
	h.mu.Unlock()
	if hook != nil {
		hook(key)
	}
	if h.failing.Load() {
		return "", errors.New("inventory backend unreachable")
	}
	version, err := h.inner.Put(ctx, key, body)
	if err == nil && h.ackLost.Load() {
		return "", errors.New("connection reset after the object was published")
	}
	return version, err
}

func (h *hookedInventory) Get(ctx context.Context, key string) ([]byte, string, error) {
	return h.inner.Get(ctx, key)
}

func (h *hookedInventory) List(ctx context.Context, prefix, cursor string, limit int) (application.InventoryPage, error) {
	if h.failing.Load() {
		return application.InventoryPage{}, errors.New("inventory backend unreachable")
	}
	return h.inner.List(ctx, prefix, cursor, limit)
}

func (h *hookedInventory) setHook(fn func(key string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onPut = fn
}

// assertNoLocksHeld takes NOWAIT row locks on the dispatch, its operation
// and its allocations from another connection: any lock still held by the
// admission would make the probe fail with lock_not_available.
func (h *hookedInventory) assertNoLocksHeld(ctx context.Context, key string) {
	var id string
	if _, err := fmt.Sscanf(key, "model-dispatch/%s", &id); err != nil {
		if _, err := fmt.Sscanf(key, "tool-dispatch/%s", &id); err != nil {
			return
		}
	}
	tx, err := h.probe.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		h.locked.Add(1)
		return
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	for _, q := range []string{
		"SELECT operation_id FROM dispatches WHERE dispatch_id = $1 FOR UPDATE NOWAIT",
		"SELECT 1 FROM operations WHERE operation_id = (SELECT operation_id FROM dispatches WHERE dispatch_id = $1) FOR UPDATE NOWAIT",
		"SELECT 1 FROM allocations WHERE operation_id = (SELECT operation_id FROM dispatches WHERE dispatch_id = $1) FOR UPDATE NOWAIT",
		"SELECT 1 FROM attempts WHERE attempt_id = (SELECT attempt_id FROM dispatches WHERE dispatch_id = $1) FOR UPDATE NOWAIT",
	} {
		rows, err := tx.Query(ctx, q, id)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
				h.locked.Add(1)
			}
			return
		}
		rows.Close()
	}
}

type dispatchProcess struct {
	pool     *pgxpool.Pool
	ops      *application.Operations
	exec     *application.Execution
	dispatch *application.Dispatch
}

func newDispatchProcess(t *testing.T, inst *testdb.Instance, inv *hookedInventory, auth *authorityDouble, prices application.PriceBook) *dispatchProcess {
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	return &dispatchProcess{
		pool:     pool,
		ops:      application.NewOperations(store, inv, []domain.Profile{profile}, domain.SystemClock{}, testLog),
		exec:     application.NewExecution(store, inv, manifests, []domain.Profile{profile}, domain.SystemClock{}, testLog),
		dispatch: application.NewDispatch(store, inv, prices, auth, development.NewNotSentEvidence(inv, []string{issuer}), domain.SystemClock{}, testLog),
	}
}

// pools inserts the current platform, tenant and actor pools of scopeA.
func pools(t *testing.T, pool *pgxpool.Pool, suffix string, caps [3]int64) {
	t.Helper()
	ctx := context.Background()
	for i, row := range []struct {
		level         string
		tenant, actor *string
	}{{"platform", nil, nil}, {"tenant", &scopeA.TenantID, nil}, {"actor", &scopeA.TenantID, &scopeA.ActorID}} {
		_, err := pool.Exec(ctx, "INSERT INTO budget_pools (pool_id, level, tenant_id, actor_id, period_start, period_end, currency, cap_amount) VALUES ($1, $2, $3, $4, now() - interval '1 hour', now() + interval '1 hour', 'USD', $5)",
			"pool-"+row.level+"-"+suffix, row.level, row.tenant, row.actor, caps[i])
		require.NoError(t, err)
	}
}

func dropPools(t *testing.T, pool *pgxpool.Pool, suffix string) {
	t.Helper()
	// Pools are deleted only by the migrator role in production; the test
	// container runs the app role, so the pools are moved out of the
	// current period instead.
	_, err := pool.Exec(context.Background(), "UPDATE budget_pools SET period_end = period_start + interval '1 second' WHERE pool_id LIKE $1", "pool-%-"+suffix)
	require.NoError(t, err)
}

// funded creates an accepted operation with an open attempt and, when
// amount is positive, allocations at every level.
func funded(t *testing.T, p *dispatchProcess, id string, amount int64) (*domain.Operation, *domain.Attempt) {
	t.Helper()
	ctx := context.Background()
	op, _, err := p.ops.Create(ctx, cmd("tenant_a", "cmd_"+id, "body"), scopeA, domain.KindLocalCheck, subject, nil)
	require.NoError(t, err)
	at, _, err := p.exec.OpenAttempt(ctx, cmd("tenant_a", "cmd_"+id+"_open", "open"), op.ID, "local-check", 0, "local-check-v1")
	require.NoError(t, err)
	if amount > 0 {
		allocs, existing, err := p.dispatch.Allocate(ctx, scopeA, op.ID, usd(amount))
		require.NoError(t, err)
		require.False(t, existing)
		require.Len(t, allocs, 3, "one allocation per level")
	}
	return op, at
}

func modelRequest(op *domain.Operation, at *domain.Attempt, callID, owner string, exposure int64) domain.AdmissionRequest {
	return domain.AdmissionRequest{Kind: domain.DispatchModel, CallID: callID, Owner: owner, OperationID: op.ID, AttemptID: at.ID, ExecutionEpoch: op.ExecutionEpoch,
		RouteID: modelRoute, Provider: modelProvider, Model: modelName, MaxExposure: usd(exposure), Deadline: time.Now().Add(time.Minute)}
}

// reported is an explicit usage report; a nil *Usage is a report without one.
func reported(u domain.Usage) *domain.Usage { return &u }

func admitCmd(callID, body string) domain.CommandIdentity {
	return cmd("tenant_a", "admit_"+callID, body)
}

func allocations(t *testing.T, pool *pgxpool.Pool, opID string) (reserved, consumed []int64) {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT reserved, consumed FROM allocations WHERE operation_id = $1 ORDER BY pool_id", opID)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var r, c int64
		require.NoError(t, rows.Scan(&r, &c))
		reserved, consumed = append(reserved, r), append(consumed, c)
	}
	return
}

func entries(t *testing.T, pool *pgxpool.Pool, dispatchID string) (kinds []string, total int64) {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT kind, amount FROM cost_entries WHERE dispatch_id = $1 AND kind <> 'estimate' ORDER BY created_at, entry_id", dispatchID)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var k string
		var a int64
		require.NoError(t, rows.Scan(&k, &a))
		kinds = append(kinds, k)
		total += a
	}
	return
}

func attestation(t *testing.T, inv *hookedInventory, d *domain.Dispatch, iss string) (ref string, digest domain.Digest) {
	t.Helper()
	body, _ := json.Marshal(development.NotSentAttestation{Class: "not-sent", DispatchID: d.ID, CallID: d.CallID, Owner: d.Owner, Issuer: iss, SealedAt: time.Now().UTC().Format(time.RFC3339)})
	ref = development.Key(d.ID)
	_, err := inv.inner.Put(context.Background(), ref, body)
	require.NoError(t, err)
	return ref, domain.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(body)))
}

func TestDispatch(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	inv := &hookedInventory{inner: fsInv, probe: inst.Pool(t)}
	auth := &authorityDouble{allowed: map[string]bool{"tenant_a/" + modelRoute: true}}
	prices := newPriceSource(fixturePrices(t, "fx-model-1", 1))
	p1, p2 := newDispatchProcess(t, inst, inv, auth, prices), newDispatchProcess(t, inst, inv, auth, prices)
	ctx := context.Background()
	pools(t, p1.pool, "main", [3]int64{1_000_000_000, 100_000_000, 10_000_000})

	t.Run("allocation at every level within shared caps, idempotent, denied when a level is missing or its cap is short", func(t *testing.T) {
		op, _ := funded(t, p1, "alloc", 0)
		allocs, existing, err := p1.dispatch.Allocate(ctx, scopeA, op.ID, usd(5000))
		require.NoError(t, err)
		require.False(t, existing)
		require.Len(t, allocs, 3)
		again, existing, err := p2.dispatch.Allocate(ctx, scopeA, op.ID, usd(5000))
		require.NoError(t, err)
		require.True(t, existing, "a repeated allocation draws nothing more")
		require.Equal(t, allocs[0].ID, again[0].ID)
		var allocated int64
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT allocated FROM budget_pools WHERE pool_id = 'pool-actor-main'").Scan(&allocated))
		require.Equal(t, int64(5000), allocated)
		got, err := p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FinanceFunded, got.Finance)

		// Two operations race for the last of a small actor pool: the pool
		// lock serializes them and exactly one fits.
		pools(t, p1.pool, "tight", [3]int64{1_000_000_000, 100_000_000, 7000})
		dropPools(t, p1.pool, "main")
		opA, _ := funded(t, p1, "tight_a", 0)
		opB, _ := funded(t, p2, "tight_b", 0)
		var wg sync.WaitGroup
		results := make(chan error, 2)
		for _, run := range []struct {
			p  *dispatchProcess
			op *domain.Operation
		}{{p1, opA}, {p2, opB}} {
			wg.Add(1)
			go func(p *dispatchProcess, op *domain.Operation) {
				defer wg.Done()
				_, _, err := p.dispatch.Allocate(ctx, scopeA, op.ID, usd(4000))
				results <- err
			}(run.p, run.op)
		}
		wg.Wait()
		close(results)
		var ok, exhausted int
		for err := range results {
			switch {
			case err == nil:
				ok++
			case errors.Is(err, domain.ErrBudgetExhausted):
				exhausted++
			default:
				t.Fatal(err)
			}
		}
		require.Equal(t, [2]int{1, 1}, [2]int{ok, exhausted})
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT allocated FROM budget_pools WHERE pool_id = 'pool-actor-tight'").Scan(&allocated))
		require.Equal(t, int64(4000), allocated, "the cap was never exceeded")
		dropPools(t, p1.pool, "tight")
		pools(t, p1.pool, "main2", [3]int64{1_000_000_000, 100_000_000, 10_000_000})
		// A scope without a current pool at some level allocates nothing.
		_, _, err = p1.dispatch.Allocate(ctx, domain.Scope{TenantID: "tenant_a", ActorID: "user_other"}, op.ID, usd(1))
		require.ErrorIs(t, err, domain.ErrBudgetExhausted)
	})

	t.Run("two processes admit the same call concurrently: one permission, one obligation, one reservation", func(t *testing.T) {
		op, at := funded(t, p1, "race", 100_000)
		req := modelRequest(op, at, "call-race", "proxy-a", 1000)
		c := admitCmd("call-race", "body")
		var wg sync.WaitGroup
		results := make(chan application.Admission, 2)
		for _, p := range []*dispatchProcess{p1, p2} {
			wg.Add(1)
			go func(p *dispatchProcess) {
				defer wg.Done()
				a, err := p.dispatch.Admit(ctx, c, req)
				require.NoError(t, err)
				results <- a
			}(p)
		}
		wg.Wait()
		close(results)
		var allowed int
		var ids []string
		for a := range results {
			if a.Allowed {
				allowed++
			}
			ids = append(ids, a.Dispatch.ID)
			require.Equal(t, domain.DispatchAuthorized, a.Dispatch.State)
		}
		require.Equal(t, 1, allowed, "exactly one first-use permission")
		require.Equal(t, ids[0], ids[1], "one dispatch record for the call")
		_, err := os.Stat(filepath.Join(fsInv.Root(), "model-dispatch", ids[0]))
		require.NoError(t, err, "the obligation was published before the permission")
		reserved, _ := allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{1000, 1000, 1000}, reserved, "one reservation at every level")
		var estimates int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM cost_entries WHERE dispatch_id = $1 AND kind = 'estimate'", ids[0]).Scan(&estimates))
		require.Equal(t, 1, estimates)
		require.Zero(t, inv.locked.Load(), "no row lock was held while the obligation was written")
	})

	t.Run("a lost first response is never recovered: retries, queries and same-owner reentry stay without permission; a changed digest conflicts", func(t *testing.T) {
		op, at := funded(t, p1, "lost", 100_000)
		req := modelRequest(op, at, "call-lost", "proxy-a", 1000)
		c := admitCmd("call-lost", "body")
		first, err := p1.dispatch.Admit(ctx, c, req)
		require.NoError(t, err)
		require.True(t, first.Allowed) // this response is what the sender never receives
		for i, p := range []*dispatchProcess{p1, p2, p1} {
			again, err := p.dispatch.Admit(ctx, c, req)
			require.NoError(t, err)
			require.False(t, again.Allowed, "retry %d restored permission", i)
			require.Equal(t, first.Dispatch.ID, again.Dispatch.ID)
			require.Equal(t, domain.DispatchAuthorized, again.Dispatch.State)
			require.Empty(t, again.DenialCode)
		}
		got, err := p2.dispatch.Get(ctx, "", "proxy-a", "call-lost")
		require.NoError(t, err)
		require.Equal(t, domain.DispatchAuthorized, got.State)
		_, err = p2.dispatch.Admit(ctx, admitCmd("call-lost", "other body"), req)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict)
		reserved, _ := allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{1000, 1000, 1000}, reserved, "the retries reserved nothing more")
		// The other owner cannot take the call either: the identity is
		// tenant/owner/call and a different owner is a different call.
		other, err := p1.dispatch.Admit(ctx, admitCmd("call-lost-b", "body"), modelRequest(op, at, "call-lost", "proxy-b", 1000))
		require.NoError(t, err)
		require.NotEqual(t, first.Dispatch.ID, other.Dispatch.ID)
	})

	t.Run("allocation races cannot exceed the allocation", func(t *testing.T) {
		op, at := funded(t, p1, "cap", 3500)
		var wg sync.WaitGroup
		var allowed atomic.Int32
		var mu sync.Mutex
		var deniedCalls []string
		requests := map[string]domain.AdmissionRequest{}
		for i := 0; i < 8; i++ {
			id := fmt.Sprintf("call-cap-%d", i)
			requests[id] = modelRequest(op, at, id, "proxy-a", 1000)
		}
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int, p *dispatchProcess) {
				defer wg.Done()
				id := fmt.Sprintf("call-cap-%d", i)
				a, err := p.dispatch.Admit(ctx, admitCmd(id, "body"), requests[id])
				require.NoError(t, err)
				if a.Allowed {
					allowed.Add(1)
					return
				}
				require.Equal(t, domain.DenyBudgetExhausted, a.DenialCode)
				require.Equal(t, domain.DispatchDenied, a.Dispatch.State)
				mu.Lock()
				deniedCalls = append(deniedCalls, id)
				mu.Unlock()
			}(i, []*dispatchProcess{p1, p2}[i%2])
		}
		wg.Wait()
		require.Equal(t, int32(3), allowed.Load(), "3 × 1000 fit into 3500, a fourth never does")
		require.Len(t, deniedCalls, 5)
		reserved, _ := allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{3000, 3000, 3000}, reserved)
		denial, err := p2.dispatch.Get(ctx, "", "proxy-a", deniedCalls[0])
		require.NoError(t, err)
		require.Equal(t, domain.DispatchDenied, denial.State)
		again, err := p1.dispatch.Admit(ctx, admitCmd(denial.CallID, "body"), requests[denial.CallID])
		require.NoError(t, err)
		require.False(t, again.Allowed, "a recorded denial is repeated, never turned into permission")
		refreshed := requests[denial.CallID]
		refreshed.Deadline = refreshed.Deadline.Add(time.Second)
		_, err = p2.dispatch.Admit(ctx, admitCmd(denial.CallID, "body"), refreshed)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "the same call with another deadline is another request, not a reentry")
	})

	t.Run("usage is deduplicated by source and sequence and charged as cumulative deltas; overspend is retained and blocks new admission", func(t *testing.T) {
		op, at := funded(t, p1, "usage", 100_000)
		a, err := p1.dispatch.Admit(ctx, admitCmd("call-usage", "body"), modelRequest(op, at, "call-usage", "proxy-a", 10_000))
		require.NoError(t, err)
		require.True(t, a.Allowed)
		id := a.Dispatch.ID
		observe := func(p *dispatchProcess, source string, seq uint64, outcome domain.DispatchOutcome, in, out uint64) (*domain.Dispatch, bool) {
			d, existing, err := p.dispatch.Observe(ctx, id, source, seq, outcome, reported(domain.Usage{Input: in, Output: out}), "native-"+fmt.Sprint(seq), time.Now())
			require.NoError(t, err)
			return d, existing
		}
		d, existing := observe(p1, "proxy", 1, domain.DispatchSucceeded, 100, 200)
		require.False(t, existing)
		require.Equal(t, domain.DispatchObserved, d.State)
		kinds, total := entries(t, p1.pool, id)
		require.Equal(t, []string{"actual"}, kinds)
		require.Equal(t, int64(300+3000), total, "100 input at 3 per unit, 200 output at 15 per unit")
		reserved, consumed := allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{0, 0, 0}, reserved, "a definite outcome releases the reservation")
		require.Equal(t, []int64{3300, 3300, 3300}, consumed)

		_, existing = observe(p2, "proxy", 1, domain.DispatchSucceeded, 100, 200)
		require.True(t, existing, "the same source and sequence is applied once")
		_, existing = observe(p2, "proxy", 3, domain.DispatchSucceeded, 300, 600)
		require.False(t, existing)
		_, existing = observe(p1, "proxy", 2, domain.DispatchSucceeded, 200, 400)
		require.False(t, existing, "an out-of-order report is recorded")
		_, _ = observe(p2, "provider-receipt", 1, domain.DispatchSucceeded, 300, 600)
		kinds, total = entries(t, p1.pool, id)
		require.Equal(t, []string{"actual", "correction"}, kinds, "the increase is one appended correction; the duplicate, the out-of-order report and the second source charged nothing")
		require.Equal(t, int64(900+9000), total, "the running maximum across sources is charged exactly once")
		var correctionOf string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT correction_of FROM cost_entries WHERE dispatch_id = $1 AND kind = 'correction'", id).Scan(&correctionOf))
		require.NotEmpty(t, correctionOf, "corrections reference the actual entry they correct")
		_, consumed = allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{9900, 9900, 9900}, consumed)
		var observations int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM usage_observations WHERE dispatch_id = $1", id).Scan(&observations))
		require.Equal(t, 4, observations, "every distinct report is kept immutably")

		// Overspend: actual cost far above the reserved exposure is charged
		// in full and fences the operation.
		small, err := p2.dispatch.Admit(ctx, admitCmd("call-over", "body"), modelRequest(op, at, "call-over", "proxy-a", 10))
		require.NoError(t, err)
		require.True(t, small.Allowed)
		_, _, err = p1.dispatch.Observe(ctx, small.Dispatch.ID, "proxy", 1, domain.DispatchSucceeded, reported(domain.Usage{Output: 1000}), "", time.Now())
		require.NoError(t, err)
		_, total = entries(t, p1.pool, small.Dispatch.ID)
		require.Equal(t, int64(15000), total, "the overspend is retained, not capped at the reservation")
		blocked, err := p2.dispatch.Admit(ctx, admitCmd("call-after-over", "body"), modelRequest(op, at, "call-after-over", "proxy-a", 10))
		require.NoError(t, err)
		require.False(t, blocked.Allowed)
		require.Equal(t, domain.DenyBudgetExhausted, blocked.DenialCode)
		_, consumed = allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{24900, 24900, 24900}, consumed)
	})

	t.Run("unknown outcome retains exposure; not-sent needs independent evidence; a replacement needs fresh admission", func(t *testing.T) {
		op, at := funded(t, p1, "unknown", 100_000)
		a, err := p1.dispatch.Admit(ctx, admitCmd("call-unk", "body"), modelRequest(op, at, "call-unk", "proxy-a", 2000))
		require.NoError(t, err)
		require.True(t, a.Allowed)
		d, _, err := p2.dispatch.Observe(ctx, a.Dispatch.ID, "proxy", 1, domain.DispatchOutcomeUnknown, nil, "", time.Now())
		require.NoError(t, err)
		require.Equal(t, domain.DispatchUnknown, d.State)
		reserved, _ := allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{2000, 2000, 2000}, reserved, "unknown exposure is not released as zero cost")
		got, err := p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FinanceExposureUnknown, got.Finance)

		c := cmd("tenant_a", "confirm_unk", "confirm")
		_, _, err = p1.dispatch.ConfirmNotSent(ctx, c, d.ID, "not-sent/"+d.ID, subject.SubjectDigest)
		require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "a reference without an object")
		wrongIssuer, wrongDigest := attestation(t, inv, d, "candidate-self-report")
		_, _, err = p1.dispatch.ConfirmNotSent(ctx, c, d.ID, wrongIssuer, wrongDigest)
		require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "an untrusted issuer")
		// Replace the attestation with a trusted one (a different body under
		// the same key is a conflict, so the object is rewritten directly).
		require.NoError(t, os.Remove(filepath.Join(fsInv.Root(), "not-sent", d.ID)))
		ref, digest := attestation(t, inv, d, issuer)
		_, _, err = p2.dispatch.ConfirmNotSent(ctx, c, d.ID, ref, "sha256:0000000000000000000000000000000000000000000000000000000000000000")
		require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "a digest that does not match the object")
		got2, err := p1.dispatch.Get(ctx, d.ID, "", "")
		require.NoError(t, err)
		require.Equal(t, domain.DispatchUnknown, got2.State, "insufficient evidence changed nothing")
		confirmed, existing, err := p2.dispatch.ConfirmNotSent(ctx, c, d.ID, ref, digest)
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.DispatchConfirmedNotSent, confirmed.State)
		reserved, _ = allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{0, 0, 0}, reserved, "confirmed not sent releases the exposure")
		got, err = p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FinanceFunded, got.Finance)
		_, existing, err = p1.dispatch.ConfirmNotSent(ctx, c, d.ID, ref, digest)
		require.NoError(t, err)
		require.True(t, existing)

		// A replacement without the confirming evidence is refused; with it
		// the replacement is a fresh single-use admission.
		bare := modelRequest(op, at, "call-unk-2", "proxy-a", 2000)
		bare.SupersedesCallID = "call-unk"
		refused, err := p1.dispatch.Admit(ctx, admitCmd("call-unk-2", "body"), bare)
		require.NoError(t, err)
		require.False(t, refused.Allowed)
		require.Equal(t, domain.DenyStaleExecution, refused.DenialCode)
		replacement := modelRequest(op, at, "call-unk-3", "proxy-a", 2000)
		replacement.SupersedesCallID, replacement.EvidenceRef = "call-unk", ref
		admitted, err := p2.dispatch.Admit(ctx, admitCmd("call-unk-3", "body"), replacement)
		require.NoError(t, err)
		require.True(t, admitted.Allowed)
		again, err := p1.dispatch.Admit(ctx, admitCmd("call-unk-3", "body"), replacement)
		require.NoError(t, err)
		require.False(t, again.Allowed)

		// A call whose usage was observed was sent: it can never be confirmed not sent.
		sent, err := p1.dispatch.Admit(ctx, admitCmd("call-sent", "body"), modelRequest(op, at, "call-sent", "proxy-a", 100))
		require.NoError(t, err)
		_, _, err = p1.dispatch.Observe(ctx, sent.Dispatch.ID, "proxy", 1, domain.DispatchSucceeded, reported(domain.Usage{Input: 1}), "", time.Now())
		require.NoError(t, err)
		sref, sdigest := attestation(t, inv, sent.Dispatch, issuer)
		_, _, err = p2.dispatch.ConfirmNotSent(ctx, cmd("tenant_a", "confirm_sent", "confirm"), sent.Dispatch.ID, sref, sdigest)
		require.ErrorIs(t, err, domain.ErrStaleExecution)
	})

	t.Run("cancel, grant revocation and expired authority racing the inventory write are rejected by the second transaction", func(t *testing.T) {
		op, at := funded(t, p1, "cancelrace", 100_000)
		inv.setHook(func(key string) {
			current, err := p2.ops.Get(ctx, scopeA, op.ID)
			require.NoError(t, err)
			_, _, err = p2.ops.SubmitCommand(ctx, cmd("tenant_a", "cmd_cancelrace_cancel", "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
			require.NoError(t, err)
		})
		a, err := p1.dispatch.Admit(ctx, admitCmd("call-cancelrace", "body"), modelRequest(op, at, "call-cancelrace", "proxy-a", 1000))
		inv.setHook(nil)
		require.NoError(t, err)
		require.False(t, a.Allowed)
		require.Equal(t, domain.DenyStaleExecution, a.DenialCode)
		require.Equal(t, domain.DispatchDenied, a.Dispatch.State)
		reserved, _ := allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{0, 0, 0}, reserved, "the prepared reservation was released")
		_, err = os.Stat(filepath.Join(fsInv.Root(), "model-dispatch", a.Dispatch.ID))
		require.NoError(t, err, "the obligation stays inventoried as a prepared-then-denied call")

		op, at = funded(t, p1, "authrace", 100_000)
		inv.setHook(func(string) { auth.expired.Store(true) })
		a, err = p2.dispatch.Admit(ctx, admitCmd("call-authrace", "body"), modelRequest(op, at, "call-authrace", "proxy-a", 1000))
		inv.setHook(nil)
		auth.expired.Store(false)
		require.NoError(t, err)
		require.False(t, a.Allowed)
		require.Equal(t, domain.DenyForbidden, a.DenialCode, "evidence that expired before the consuming transaction denies")

		op, at = funded(t, p1, "grantrace", 100_000)
		_, err = p1.pool.Exec(ctx, "INSERT INTO grant_policies (grant_id, grant_revision, tenant_id, policy_digest, server_id, methods, cost_cap_currency, cost_cap_amount, policy_epoch, receipt_id, revocation_state) VALUES ('grant-race', 1, 'tenant_a', $1, 'srv', ARRAY['read'], 'USD', 100000, 1, 'rcpt-race', 'none')", string(subject.SubjectDigest))
		require.NoError(t, err)
		tool := domain.AdmissionRequest{Kind: domain.DispatchTool, CallID: "call-grantrace", Owner: "mcp-a", OperationID: op.ID, AttemptID: at.ID, ExecutionEpoch: op.ExecutionEpoch,
			RouteID: toolRoute, GrantID: "grant-race", GrantRevision: 1, ServerID: "srv", Method: "read", MaxExposure: usd(500), Deadline: time.Now().Add(time.Minute)}
		inv.setHook(func(string) {
			_, err := p2.pool.Exec(ctx, "UPDATE grant_policies SET revocation_state = 'fenced', fenced_at = now() WHERE grant_id = 'grant-race'")
			require.NoError(t, err)
		})
		a, err = p1.dispatch.Admit(ctx, admitCmd("call-grantrace", "body"), tool)
		inv.setHook(nil)
		require.NoError(t, err)
		require.False(t, a.Allowed)
		require.Equal(t, domain.DenyForbidden, a.DenialCode, "a revocation fence installed during the inventory write is seen by the recheck")
		require.Zero(t, inv.locked.Load(), "no row lock was held while the hooks ran")
	})

	t.Run("failed or uncertain inventory writes grant no permission and the same call completes later", func(t *testing.T) {
		op, at := funded(t, p1, "inv", 100_000)
		req := modelRequest(op, at, "call-inv", "proxy-a", 1000)
		c := admitCmd("call-inv", "body")
		inv.failing.Store(true)
		_, err := p1.dispatch.Admit(ctx, c, req)
		require.ErrorIs(t, err, domain.ErrEffectUncertain)
		inv.failing.Store(false)
		var state, inventoryState string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT state, inventory_state FROM dispatches WHERE call_id = 'call-inv'").Scan(&state, &inventoryState))
		require.Equal(t, [2]string{"prepared", "uncertain"}, [2]string{state, inventoryState})
		reserved, _ := allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{1000, 1000, 1000}, reserved, "the reservation of the prepared call is kept")
		a, err := p2.dispatch.Admit(ctx, c, req)
		require.NoError(t, err)
		require.True(t, a.Allowed, "the same call completes its sequence once the inventory answers")
		require.Equal(t, domain.InventoryConfirmed, a.Dispatch.Inventory)

		req2 := modelRequest(op, at, "call-inv-2", "proxy-a", 1000)
		c2 := admitCmd("call-inv-2", "body")
		inv.ackLost.Store(true)
		_, err = p1.dispatch.Admit(ctx, c2, req2)
		require.ErrorIs(t, err, domain.ErrEffectUncertain)
		inv.ackLost.Store(false)
		var id string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT dispatch_id FROM dispatches WHERE call_id = 'call-inv-2'").Scan(&id))
		_, err = os.Stat(filepath.Join(fsInv.Root(), "model-dispatch", id))
		require.NoError(t, err, "the object was published before the acknowledgement was lost")
		a, err = p2.dispatch.Admit(ctx, c2, req2)
		require.NoError(t, err)
		require.True(t, a.Allowed, "the published object is idempotent and the sequence completes once")
		a, err = p1.dispatch.Admit(ctx, c2, req2)
		require.NoError(t, err)
		require.False(t, a.Allowed)
	})

	t.Run("missing price, allocation, authority or grant denies; cross-tenant is not found; a granted tool is admitted once", func(t *testing.T) {
		op, at := funded(t, p1, "deny", 100_000)
		noPrice := modelRequest(op, at, "call-noprice", "proxy-a", 1000)
		noPrice.RouteID = "unpriced-route"
		a, err := p1.dispatch.Admit(ctx, admitCmd("call-noprice", "body"), noPrice)
		require.NoError(t, err)
		require.Equal(t, domain.DenyProfileUnqualified, a.DenialCode)
		auth.denied.Store(true)
		a, err = p2.dispatch.Admit(ctx, admitCmd("call-noauth", "body"), modelRequest(op, at, "call-noauth", "proxy-a", 1000))
		auth.denied.Store(false)
		require.NoError(t, err)
		require.Equal(t, domain.DenyForbidden, a.DenialCode)
		unbounded := modelRequest(op, at, "call-unbounded", "proxy-a", 1_000_001)
		a, err = p1.dispatch.Admit(ctx, admitCmd("call-unbounded", "body"), unbounded)
		require.NoError(t, err)
		require.Equal(t, domain.DenyProfileUnqualified, a.DenialCode, "exposure above the route bound is unbounded exposure")
		unfunded, unfundedAt := funded(t, p2, "unfunded", 0)
		a, err = p1.dispatch.Admit(ctx, admitCmd("call-unfunded", "body"), modelRequest(unfunded, unfundedAt, "call-unfunded", "proxy-a", 1))
		require.NoError(t, err)
		require.Equal(t, domain.DenyBudgetExhausted, a.DenialCode)
		_, err = p2.dispatch.Admit(ctx, cmd("tenant_b", "admit_x", "body"), modelRequest(op, at, "call-x", "proxy-a", 1))
		require.ErrorIs(t, err, domain.ErrNotFound)

		tool := domain.AdmissionRequest{Kind: domain.DispatchTool, CallID: "call-tool", Owner: "mcp-a", OperationID: op.ID, AttemptID: at.ID, ExecutionEpoch: op.ExecutionEpoch,
			RouteID: toolRoute, GrantID: "grant-1", GrantRevision: 1, ServerID: "srv", Method: "read", MaxExposure: usd(500), Deadline: time.Now().Add(time.Minute)}
		a, err = p1.dispatch.Admit(ctx, admitCmd("call-tool", "body"), tool)
		require.NoError(t, err)
		require.Equal(t, domain.DenyForbidden, a.DenialCode, "no registered grant revision")
		_, err = p1.pool.Exec(ctx, "INSERT INTO grant_policies (grant_id, grant_revision, tenant_id, policy_digest, server_id, methods, cost_cap_currency, cost_cap_amount, policy_epoch, receipt_id, revocation_state) VALUES ('grant-1', 1, 'tenant_a', $1, 'srv', ARRAY['read'], 'USD', 100000, 1, 'rcpt-1', 'none')", string(subject.SubjectDigest))
		require.NoError(t, err)
		write := tool
		write.CallID, write.Method, write.RouteID = "call-tool-write", "write", "srv/write"
		a, err = p2.dispatch.Admit(ctx, admitCmd("call-tool-write", "body"), write)
		require.NoError(t, err)
		require.Equal(t, domain.DenyProfileUnqualified, a.DenialCode, "an unpriced tool method is denied before the grant is consulted")
		tool.CallID = "call-tool-2"
		a, err = p2.dispatch.Admit(ctx, admitCmd("call-tool-2", "body"), tool)
		require.NoError(t, err)
		require.True(t, a.Allowed)
		require.Equal(t, "fx-tool-1", a.Dispatch.MeterRevision)
		_, err = os.Stat(filepath.Join(fsInv.Root(), "tool-dispatch", a.Dispatch.ID))
		require.NoError(t, err)
		again, err := p1.dispatch.Admit(ctx, admitCmd("call-tool-2", "body"), tool)
		require.NoError(t, err)
		require.False(t, again.Allowed)
		require.Zero(t, inv.locked.Load(), "no row lock was ever held during an inventory write")
	})

	t.Run("a prepared call binds its recorded request: another operation cannot reenter it, the original request still completes once, and the recheck runs against the persisted binding", func(t *testing.T) {
		opA, atA := funded(t, p1, "bind_a", 100_000)
		opB, atB := funded(t, p2, "bind_b", 100_000)
		req := modelRequest(opA, atA, "call-bind", "proxy-a", 1000)
		c := admitCmd("call-bind", "body")
		inv.failing.Store(true)
		_, err := p1.dispatch.Admit(ctx, c, req)
		require.ErrorIs(t, err, domain.ErrEffectUncertain)
		inv.failing.Store(false)
		prepared, err := p2.dispatch.Get(ctx, "", "proxy-a", "call-bind")
		require.NoError(t, err)
		require.Equal(t, domain.DispatchPrepared, prepared.State)
		require.Equal(t, opA.ID, prepared.OperationID)
		// Operation A is cancelled while the call is still prepared.
		current, err := p1.ops.Get(ctx, scopeA, opA.ID)
		require.NoError(t, err)
		_, _, err = p1.ops.SubmitCommand(ctx, cmd("tenant_a", "cmd_bind_a_cancel", "cancel"), scopeA, opA.ID, domain.CommandCancel, current.Revision, "")
		require.NoError(t, err)

		// The same owner, call and digest now name operation B, which is
		// open and funded: that is not a reentry of the recorded call.
		borrowed := req
		borrowed.OperationID, borrowed.AttemptID, borrowed.ExecutionEpoch = opB.ID, atB.ID, opB.ExecutionEpoch
		for i, p := range []*dispatchProcess{p2, p1} {
			_, err := p.dispatch.Admit(ctx, c, borrowed)
			require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "process %d let another operation reenter the prepared call", i)
		}
		for name, mutate := range map[string]func(*domain.AdmissionRequest){
			"attempt":  func(r *domain.AdmissionRequest) { r.AttemptID = atB.ID },
			"exposure": func(r *domain.AdmissionRequest) { r.MaxExposure = usd(1) },
			"route":    func(r *domain.AdmissionRequest) { r.RouteID = "other-route" },
			"deadline": func(r *domain.AdmissionRequest) { r.Deadline = r.Deadline.Add(time.Second) },
			"model":    func(r *domain.AdmissionRequest) { r.Model = "other-model" },
		} {
			other := req
			mutate(&other)
			_, err := p2.dispatch.Admit(ctx, c, other)
			require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "a changed %s reentered the prepared call", name)
		}
		still, err := p1.dispatch.Get(ctx, prepared.ID, "", "")
		require.NoError(t, err)
		require.Equal(t, domain.DispatchPrepared, still.State, "the refused requests changed nothing")
		require.Equal(t, domain.InventoryUncertain, still.Inventory)
		reservedB, _ := allocations(t, p1.pool, opB.ID)
		require.Equal(t, []int64{0, 0, 0}, reservedB, "operation B reserved nothing for a call that is not its own")
		reservedA, _ := allocations(t, p1.pool, opA.ID)
		require.Equal(t, []int64{1000, 1000, 1000}, reservedA, "the prepared call still holds its exposure on A")

		// The original request reenters: the inventory answers now, and the
		// consuming transaction checks the persisted call's own operation,
		// which is fenced. No permission, the reservation is released.
		a, err := p2.dispatch.Admit(ctx, c, req)
		require.NoError(t, err)
		require.False(t, a.Allowed)
		require.Equal(t, domain.DenyStaleExecution, a.DenialCode, "the fence of the recorded operation, not the authority of another, decides")
		require.Equal(t, prepared.ID, a.Dispatch.ID)
		reservedA, _ = allocations(t, p1.pool, opA.ID)
		require.Equal(t, []int64{0, 0, 0}, reservedA)
		_, err = os.Stat(filepath.Join(fsInv.Root(), "model-dispatch", prepared.ID))
		require.NoError(t, err, "the obligation of the prepared call was published before the recheck")

		// The valid recovery path: a prepared call whose inventory write
		// failed completes its own sequence once when the same request
		// returns, and only that request is told so.
		opC, atC := funded(t, p1, "bind_c", 100_000)
		reqC := modelRequest(opC, atC, "call-bind-c", "proxy-a", 1000)
		cC := admitCmd("call-bind-c", "body")
		inv.failing.Store(true)
		_, err = p1.dispatch.Admit(ctx, cC, reqC)
		require.ErrorIs(t, err, domain.ErrEffectUncertain)
		inv.failing.Store(false)
		recovered, err := p2.dispatch.Admit(ctx, cC, reqC)
		require.NoError(t, err)
		require.True(t, recovered.Allowed, "the original request obtains its one permission after inventory recovery")
		require.Equal(t, domain.InventoryConfirmed, recovered.Dispatch.Inventory)
		again, err := p1.dispatch.Admit(ctx, cC, reqC)
		require.NoError(t, err)
		require.False(t, again.Allowed)
		require.Zero(t, inv.locked.Load(), "no row lock was held during any inventory write")
	})

	t.Run("a definite outcome without usage is refused and keeps the exposure; unknown without usage is recorded; explicit zero usage settles; explicitly reported usage was sent", func(t *testing.T) {
		op, at := funded(t, p1, "nousage", 100_000)
		a, err := p1.dispatch.Admit(ctx, admitCmd("call-nousage", "body"), modelRequest(op, at, "call-nousage", "proxy-a", 2000))
		require.NoError(t, err)
		require.True(t, a.Allowed)
		id := a.Dispatch.ID
		observations := func(id string) (rows int, reported []bool) {
			t.Helper()
			r, err := p1.pool.Query(ctx, "SELECT usage_reported FROM usage_observations WHERE dispatch_id = $1 ORDER BY sequence", id)
			require.NoError(t, err)
			defer r.Close()
			for r.Next() {
				var b bool
				require.NoError(t, r.Scan(&b))
				rows++
				reported = append(reported, b)
			}
			return
		}
		for _, outcome := range []domain.DispatchOutcome{domain.DispatchSucceeded, domain.DispatchFailed, domain.DispatchCanceled} {
			_, _, err := p2.dispatch.Observe(ctx, id, "proxy", 1, outcome, nil, "", time.Now())
			require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "%s without usage is not settlement evidence", outcome)
		}
		got, err := p1.dispatch.Get(ctx, id, "", "")
		require.NoError(t, err)
		require.Equal(t, domain.DispatchAuthorized, got.State, "a refused report leaves the call as it is")
		reserved, _ := allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{2000, 2000, 2000}, reserved, "the exposure is not released as zero cost")
		rows, _ := observations(id)
		require.Zero(t, rows, "a refused report is not recorded")

		// Unknown without usage is a report: recorded once, exposure retained.
		d, existing, err := p1.dispatch.Observe(ctx, id, "proxy", 1, domain.DispatchOutcomeUnknown, nil, "", time.Now())
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.DispatchUnknown, d.State)
		_, existing, err = p2.dispatch.Observe(ctx, id, "proxy", 1, domain.DispatchOutcomeUnknown, nil, "", time.Now())
		require.NoError(t, err)
		require.True(t, existing, "the same source and sequence is applied once")
		rows, reportedFlags := observations(id)
		require.Equal(t, 1, rows)
		require.Equal(t, []bool{false}, reportedFlags, "the report is kept as one without usage")
		reserved, consumed := allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{2000, 2000, 2000}, reserved)
		require.Equal(t, []int64{0, 0, 0}, consumed)
		opState, err := p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FinanceExposureUnknown, opState.Finance)
		_, _, err = p2.dispatch.Observe(ctx, id, "proxy", 2, domain.DispatchSucceeded, nil, "", time.Now())
		require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "still no settlement without usage")

		// The corrected report under the same sequence carries explicit
		// zero usage: that is a statement, it settles at zero cost.
		d, existing, err = p2.dispatch.Observe(ctx, id, "proxy", 2, domain.DispatchSucceeded, reported(domain.Usage{}), "", time.Now())
		require.NoError(t, err)
		require.False(t, existing, "the refused report was never recorded, so the corrected one is not a duplicate")
		require.Equal(t, domain.DispatchObserved, d.State)
		reserved, consumed = allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{0, 0, 0}, reserved, "explicitly reported zero usage releases the reservation")
		require.Equal(t, []int64{0, 0, 0}, consumed)
		kinds, total := entries(t, p1.pool, id)
		require.Empty(t, kinds)
		require.Zero(t, total)
		rows, reportedFlags = observations(id)
		require.Equal(t, 2, rows)
		require.Equal(t, []bool{false, true}, reportedFlags)
		opState, err = p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FinanceFunded, opState.Finance)
		_, existing, err = p1.dispatch.Observe(ctx, id, "proxy", 3, domain.DispatchFailed, nil, "", time.Now())
		require.NoError(t, err, "a later report without usage on an observed call is recorded and changes nothing")
		require.False(t, existing)

		// Explicitly reported usage — even zero — means the provider
		// answered: such a call was sent and is never confirmed not sent,
		// while a call that only ever reported unknown without usage can be.
		zero, err := p2.dispatch.Admit(ctx, admitCmd("call-zero-unk", "body"), modelRequest(op, at, "call-zero-unk", "proxy-a", 100))
		require.NoError(t, err)
		require.True(t, zero.Allowed)
		_, _, err = p1.dispatch.Observe(ctx, zero.Dispatch.ID, "proxy", 1, domain.DispatchOutcomeUnknown, reported(domain.Usage{}), "", time.Now())
		require.NoError(t, err)
		zref, zdigest := attestation(t, inv, zero.Dispatch, issuer)
		_, _, err = p2.dispatch.ConfirmNotSent(ctx, cmd("tenant_a", "confirm_zero_unk", "confirm"), zero.Dispatch.ID, zref, zdigest)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "explicitly reported zero usage is an observation of a send")
		reserved, _ = allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{100, 100, 100}, reserved, "its exposure stays retained")
		none, err := p1.dispatch.Admit(ctx, admitCmd("call-none-unk", "body"), modelRequest(op, at, "call-none-unk", "proxy-a", 100))
		require.NoError(t, err)
		require.True(t, none.Allowed)
		_, _, err = p2.dispatch.Observe(ctx, none.Dispatch.ID, "proxy", 1, domain.DispatchOutcomeUnknown, nil, "", time.Now())
		require.NoError(t, err)
		nref, ndigest := attestation(t, inv, none.Dispatch, issuer)
		confirmed, _, err := p1.dispatch.ConfirmNotSent(ctx, cmd("tenant_a", "confirm_none_unk", "confirm"), none.Dispatch.ID, nref, ndigest)
		require.NoError(t, err)
		require.Equal(t, domain.DispatchConfirmedNotSent, confirmed.State)
		reserved, _ = allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{100, 100, 100}, reserved, "only the confirmed call released its 100")
	})

	t.Run("provider and model are checked against the trusted route; the price revision frozen at admission meters the call after the route is repriced", func(t *testing.T) {
		op, at := funded(t, p1, "binding", 100_000)
		for name, mutate := range map[string]struct {
			apply func(*domain.AdmissionRequest)
			code  string
		}{
			"provider": {func(r *domain.AdmissionRequest) { r.Provider = "other-provider" }, domain.DenyProfileUnqualified},
			"model":    {func(r *domain.AdmissionRequest) { r.Model = "other-model" }, domain.DenyProfileUnqualified},
			"empty":    {func(r *domain.AdmissionRequest) { r.Provider, r.Model = "", "" }, domain.DenyInvalidArgument},
		} {
			req := modelRequest(op, at, "call-binding-"+name, "proxy-a", 1000)
			mutate.apply(&req)
			a, err := p1.dispatch.Admit(ctx, admitCmd(req.CallID, "body"), req)
			require.NoError(t, err)
			require.False(t, a.Allowed)
			require.Equal(t, mutate.code, a.DenialCode, "%s", name)
			require.Equal(t, domain.DispatchDenied, a.Dispatch.State)
			again, err := p2.dispatch.Admit(ctx, admitCmd(req.CallID, "body"), req)
			require.NoError(t, err)
			require.Equal(t, mutate.code, again.DenialCode, "the denial is repeated under the call identity")
		}
		reserved, _ := allocations(t, p1.pool, op.ID)
		require.Equal(t, []int64{0, 0, 0}, reserved, "a mismatched binding reserves nothing")

		req := modelRequest(op, at, "call-binding-ok", "proxy-a", 1000)
		a, err := p2.dispatch.Admit(ctx, admitCmd("call-binding-ok", "body"), req)
		require.NoError(t, err)
		require.True(t, a.Allowed)
		require.Equal(t, "fx-model-1", a.Dispatch.MeterRevision)
		other := req
		other.Model = "other-model"
		_, err = p1.dispatch.Admit(ctx, admitCmd("call-binding-ok", "body"), other)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "the authorized call was admitted for another model")

		// A call prepared under the first observation, then the route is
		// repriced before its inventory answers.
		frozen := modelRequest(op, at, "call-frozen", "proxy-a", 1000)
		cFrozen := admitCmd("call-frozen", "body")
		inv.failing.Store(true)
		_, err = p1.dispatch.Admit(ctx, cFrozen, frozen)
		require.ErrorIs(t, err, domain.ErrEffectUncertain)
		inv.failing.Store(false)
		prices.publish(fixturePrices(t, "fx-model-2", 10))
		recovered, err := p2.dispatch.Admit(ctx, cFrozen, frozen)
		require.NoError(t, err)
		require.True(t, recovered.Allowed, "the prepared call completes under its own observation")
		require.Equal(t, "fx-model-1", recovered.Dispatch.MeterRevision, "the reentry never re-freezes the price")
		for _, id := range []string{a.Dispatch.ID, recovered.Dispatch.ID} {
			_, _, err = p1.dispatch.Observe(ctx, id, "proxy", 1, domain.DispatchSucceeded, reported(domain.Usage{Input: 100}), "", time.Now())
			require.NoError(t, err)
			_, total := entries(t, p1.pool, id)
			require.Equal(t, int64(300), total, "100 input units at the frozen 3 per unit, not the current 30")
			var costRevision string
			require.NoError(t, p1.pool.QueryRow(ctx, "SELECT cost_revision FROM usage_observations WHERE dispatch_id = $1", id).Scan(&costRevision))
			require.Equal(t, "fx-model-1", costRevision)
		}
		fresh, err := p1.dispatch.Admit(ctx, admitCmd("call-repriced", "body"), modelRequest(op, at, "call-repriced", "proxy-a", 1000))
		require.NoError(t, err)
		require.True(t, fresh.Allowed)
		require.Equal(t, "fx-model-2", fresh.Dispatch.MeterRevision, "a new call is metered under the current observation")
		_, _, err = p2.dispatch.Observe(ctx, fresh.Dispatch.ID, "proxy", 1, domain.DispatchSucceeded, reported(domain.Usage{Input: 100}), "", time.Now())
		require.NoError(t, err)
		_, total := entries(t, p1.pool, fresh.Dispatch.ID)
		require.Equal(t, int64(3000), total)
		require.Zero(t, inv.locked.Load())
	})

}
