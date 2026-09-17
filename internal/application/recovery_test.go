package application_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/development"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// p07Process is one Control replica with the P07 use cases on top of the
// P06 harness: effects, recovery and the DEVELOPMENT_ONLY outcome-query and
// disposition-evidence doubles reading attestations from the inventory.
type p07Process struct {
	*dispatchProcess
	effects  *application.Effects
	recovery *application.Recovery
}

func newP07Process(t *testing.T, inst *testdb.Instance, inv *hookedInventory, auth *authorityDouble, prices application.PriceBook) *p07Process {
	base := newDispatchProcess(t, inst, inv, auth, prices)
	store := postgres.NewStore(base.pool)
	queries := development.NewOutcomeQuery(inv, []string{issuer})
	effects := application.NewEffects(store, inv, queries, domain.SystemClock{}, testLog)
	recovery := application.NewRecovery(store, inv, queries, base.dispatch, effects, base.exec, []domain.Profile{profile}, auth,
		development.NewNotSentEvidence(inv, []string{issuer}), development.NewDispositionEvidence(inv, []string{issuer}), domain.SystemClock{}, testLog, 2)
	return &p07Process{dispatchProcess: base, effects: effects, recovery: recovery}
}

// admin connects to the Control database as the superuser: the only way a
// test can take rows away, which is how a PITR to an earlier point is
// modeled (the inventory keeps what the database lost).
func admin(t *testing.T, inst *testdb.Instance) *pgx.Conn {
	t.Helper()
	dsn := strings.Replace(inst.AdminDSN, "/postgres?", "/anvilkit_control?", 1)
	conn, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func effectRequest(op *domain.Operation, at *domain.Attempt, owner string, occurrence uint64) domain.EffectRequest {
	attemptID := ""
	if at != nil {
		attemptID = at.ID
	}
	return domain.EffectRequest{
		OperationID: op.ID, AttemptID: attemptID, Owner: owner, Kind: domain.EffectBusinessWrite, Occurrence: occurrence, CanonicalSubject: "component/c1/page/p1",
		ExpectedRevision: "7", ExecutionEpoch: op.ExecutionEpoch, LeaseID: "lease-1", LeaseFence: 3, LeaseExpiresAt: time.Now().Add(time.Minute), Deadline: time.Now().Add(time.Minute),
	}
}

func effectCmd(id, body string) domain.CommandIdentity {
	c := cmd("tenant_a", "effect_"+id, body)
	return domain.CommandIdentity{TenantID: c.TenantID, CommandID: c.CommandID, ActorID: c.ActorID, RequestDigest: c.RequestDigest}
}

func publish(t *testing.T, inv *hookedInventory, key string, v any) domain.Digest {
	t.Helper()
	body, _ := json.Marshal(v)
	_, err := inv.inner.Put(context.Background(), key, body)
	require.NoError(t, err)
	return domain.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(body)))
}

func TestEffects(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(t.TempDir())
	require.NoError(t, err)
	inv := &hookedInventory{inner: fsInv}
	auth := &authorityDouble{allowed: map[string]bool{"tenant_a/" + modelRoute: true}}
	prices := newPriceSource(fixturePrices(t, "fx-model-e", 1))
	p1, p2 := newP07Process(t, inst, inv, auth, prices), newP07Process(t, inst, inv, auth, prices)
	ctx := context.Background()
	pools(t, p1.pool, "effects", [3]int64{1_000_000_000, 100_000_000, 10_000_000})

	t.Run("the same prepare on two processes consumes one permit and reenters without a second one", func(t *testing.T) {
		op, at := funded(t, p1.dispatchProcess, "eff_dup", 0)
		c, req := effectCmd("dup", "body"), effectRequest(op, at, "activity-worker-1", 1)
		results := make(chan application.Permit, 2)
		var wg sync.WaitGroup
		for _, p := range []*p07Process{p1, p2} {
			wg.Add(1)
			go func(p *p07Process) {
				defer wg.Done()
				permit, err := p.effects.Prepare(ctx, c, req)
				require.NoError(t, err)
				results <- permit
			}(p)
		}
		wg.Wait()
		close(results)
		permitted, ids := 0, map[string]bool{}
		for r := range results {
			ids[r.Effect.ID] = true
			if r.Permitted {
				permitted++
			}
		}
		require.Len(t, ids, 1, "one obligation for one command")
		require.Equal(t, 1, permitted, "exactly one caller holds the permit")
		again, err := p2.effects.Prepare(ctx, c, req)
		require.NoError(t, err)
		require.False(t, again.Permitted, "a lost response is never a second permit")
		require.Equal(t, domain.EffectPermitted, again.Effect.State)
		require.Equal(t, domain.InventoryConfirmed, again.Effect.Inventory)
		_, _, err = inv.inner.Get(ctx, application.ObligationKey(application.ClassBusinessWrite, again.Effect.ID))
		require.NoError(t, err, "the business-write obligation is published before the permit")

		changed := req
		changed.CanonicalSubject = "component/c1/page/p2"
		_, err = p1.effects.Prepare(ctx, c, changed)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "a changed binding under the same command conflicts")
		_, err = p1.effects.Prepare(ctx, effectCmd("dup2", "other"), req)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "one obligation per (operation, kind, occurrence)")
		got, err := p2.effects.Get(ctx, "tenant_a", "", op.ID, domain.EffectBusinessWrite, 1)
		require.NoError(t, err)
		require.Equal(t, again.Effect.ID, got.ID, "the original-result query finds the mutation by its identity")
		_, err = p2.effects.Get(ctx, "tenant_b", got.ID, "", "", 0)
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("a lost inventory acknowledgement grants nothing and the reentry permits once", func(t *testing.T) {
		t.Cleanup(func() { inv.ackLost.Store(false); inv.failing.Store(false) })
		op, at := funded(t, p1.dispatchProcess, "eff_ack", 0)
		c, req := effectCmd("ack", "body"), effectRequest(op, at, "activity-worker-1", 1)
		inv.ackLost.Store(true)
		_, err := p1.effects.Prepare(ctx, c, req)
		require.ErrorIs(t, err, domain.ErrEffectUncertain)
		inv.ackLost.Store(false)
		pending, err := p2.effects.Get(ctx, "tenant_a", "", op.ID, domain.EffectBusinessWrite, 1)
		require.NoError(t, err)
		require.Equal(t, domain.EffectPrepared, pending.State, "no permit was issued")
		require.Equal(t, domain.InventoryUncertain, pending.Inventory, "the write is possibly present, never absent")
		permit, err := p2.effects.Prepare(ctx, c, req)
		require.NoError(t, err)
		require.True(t, permit.Permitted, "the reentry completes the same sequence once")
		inv.failing.Store(true)
		_, err = p1.effects.Prepare(ctx, effectCmd("down", "body"), effectRequest(op, at, "activity-worker-1", 2))
		require.ErrorIs(t, err, domain.ErrEffectUncertain, "an unavailable inventory grants nothing")
		inv.failing.Store(false)
	})

	t.Run("a fenced or expired binding is recorded as a denial that reenters as the same denial", func(t *testing.T) {
		op, at := funded(t, p1.dispatchProcess, "eff_deny", 0)
		req := effectRequest(op, at, "activity-worker-1", 1)
		req.LeaseExpiresAt = time.Now().Add(-time.Second)
		permit, err := p1.effects.Prepare(ctx, effectCmd("deny", "body"), req)
		require.NoError(t, err)
		require.False(t, permit.Permitted)
		require.Equal(t, domain.EffectDenied, permit.Effect.State)
		require.Equal(t, domain.DenyStaleExecution, permit.DenialCode)
		again, err := p2.effects.Prepare(ctx, effectCmd("deny", "body"), req)
		require.NoError(t, err)
		require.Equal(t, permit.Effect.ID, again.Effect.ID)
		require.Equal(t, domain.DenyStaleExecution, again.DenialCode)
		current, err := p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		_, _, err = p1.ops.SubmitCommand(ctx, cmd("tenant_a", "cancel_deny", "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
		require.NoError(t, err)
		fenced, err := p1.effects.Prepare(ctx, effectCmd("deny2", "body"), effectRequest(op, at, "activity-worker-1", 2))
		require.NoError(t, err)
		require.Equal(t, domain.DenyStaleExecution, fenced.DenialCode, "cancel fences new business writes; issued permits are kept")
	})

	t.Run("observations are idempotent, unknown retains the obligation and a late outcome settles it once", func(t *testing.T) {
		op, at := funded(t, p1.dispatchProcess, "eff_obs", 0)
		permit, err := p1.effects.Prepare(ctx, effectCmd("obs", "body"), effectRequest(op, at, "activity-worker-1", 1))
		require.NoError(t, err)
		require.True(t, permit.Permitted)
		id := permit.Effect.ID
		_, _, err = p1.effects.Observe(ctx, "tenant_a", id, "worker", 1, domain.EffectOutcomeSucceeded, "", "", time.Now())
		require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "a definite outcome without a receipt is not evidence")
		e, existing, err := p2.effects.Observe(ctx, "tenant_a", id, "worker", 1, domain.EffectOutcomeUnknown, "", "", time.Now())
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.EffectUnknown, e.State)
		require.True(t, e.Unresolved())
		_, existing, err = p1.effects.Observe(ctx, "tenant_a", id, "worker", 1, domain.EffectOutcomeSucceeded, "sha256:receipt", "", time.Now())
		require.NoError(t, err)
		require.True(t, existing, "the same source sequence is never applied twice, whatever it says")
		e, _, err = p1.effects.Observe(ctx, "tenant_a", id, "worker", 2, domain.EffectOutcomeFailed, "rcpt-2", "native-9", time.Now())
		require.NoError(t, err)
		require.Equal(t, domain.EffectFailed, e.State)
		require.Equal(t, "rcpt-2", e.OutcomeRef)
		e, _, err = p2.effects.Observe(ctx, "tenant_a", id, "worker", 3, domain.EffectOutcomeSucceeded, "rcpt-3", "", time.Now())
		require.NoError(t, err)
		require.Equal(t, domain.EffectFailed, e.State, "the first definite outcome is kept; a later report changes nothing")
		require.Equal(t, "rcpt-2", e.OutcomeRef)
		_, _, err = p1.effects.Observe(ctx, "tenant_b", id, "worker", 4, domain.EffectOutcomeUnknown, "", "", time.Now())
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("the original-identity query records only a trusted upstream answer", func(t *testing.T) {
		t.Cleanup(func() { inv.failing.Store(false) })
		op, at := funded(t, p1.dispatchProcess, "eff_query", 0)
		permit, err := p1.effects.Prepare(ctx, effectCmd("query", "body"), effectRequest(op, at, "activity-worker-1", 1))
		require.NoError(t, err)
		id := permit.Effect.ID
		e, report, err := p1.effects.QueryOriginal(ctx, "tenant_a", id)
		require.NoError(t, err)
		require.False(t, report.Known, "an upstream without a record establishes nothing")
		require.Equal(t, domain.EffectPermitted, e.State)
		publish(t, inv, development.OutcomeKey("business-write", id), development.OutcomeAttestation{Class: "outcome", ObligationClass: "business-write", ObligationID: id, Issuer: "stranger", Sequence: 1, Outcome: "succeeded", ReceiptRef: "rcpt", ObservedAt: time.Now().UTC().Format(time.RFC3339)})
		_, _, err = p2.effects.QueryOriginal(ctx, "tenant_a", id)
		require.ErrorIs(t, err, domain.ErrEvidenceInsufficient, "an untrusted issuer is not an upstream answer")
		e, err = p2.effects.Get(ctx, "tenant_a", id, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, domain.EffectPermitted, e.State, "nothing was recorded from it")
		second, err := p1.effects.Prepare(ctx, effectCmd("query2", "body"), effectRequest(op, at, "activity-worker-1", 2))
		require.NoError(t, err)
		require.True(t, second.Permitted)
		publish(t, inv, development.OutcomeKey("business-write", second.Effect.ID), development.OutcomeAttestation{Class: "outcome", ObligationClass: "business-write", ObligationID: second.Effect.ID, Issuer: issuer, Sequence: 1, Outcome: "succeeded", ReceiptRef: "rcpt-q", ObservedAt: time.Now().UTC().Format(time.RFC3339)})
		e, report, err = p2.effects.QueryOriginal(ctx, "tenant_a", second.Effect.ID)
		require.NoError(t, err)
		require.True(t, report.Known)
		require.Equal(t, domain.EffectSucceeded, e.State, "a trusted upstream answer is recorded as the observation")
		require.Equal(t, "rcpt-q", e.OutcomeRef)
		// cancel stays pending while a permitted write is unresolved
		current, err := p1.ops.Get(ctx, scopeA, op.ID)
		require.NoError(t, err)
		got, _, err := p1.ops.SubmitCommand(ctx, cmd("tenant_a", "cancel_query", "cancel"), scopeA, op.ID, domain.CommandCancel, current.Revision, "")
		require.NoError(t, err)
		require.Equal(t, domain.OutcomePending, got.Outcome, "senders are not quiescent while a permitted mutation is unresolved")
	})

	t.Run("a PREPARED reentry is bound to the persisted lease and its expiry, checked as of the decision", func(t *testing.T) {
		t.Cleanup(func() { inv.ackLost.Store(false) })
		op, at := funded(t, p1.dispatchProcess, "eff_lease", 0)
		req := effectRequest(op, at, "activity-worker-1", 1)
		req.LeaseExpiresAt = time.Now().Add(1500 * time.Millisecond)
		c := effectCmd("lease", "body")
		inv.ackLost.Store(true)
		_, err := p1.effects.Prepare(ctx, c, req)
		require.ErrorIs(t, err, domain.ErrEffectUncertain, "the inventory receipt is lost; the effect stays PREPARED")
		inv.ackLost.Store(false)
		cleared := req
		cleared.LeaseExpiresAt = time.Time{}
		_, err = p2.effects.Prepare(ctx, c, cleared)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "clearing the lease expiry is another binding, never a way to a permit")
		extended := req
		extended.LeaseExpiresAt = req.LeaseExpiresAt.Add(time.Hour)
		_, err = p2.effects.Prepare(ctx, c, extended)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "extending it is another binding")
		time.Sleep(time.Until(req.LeaseExpiresAt) + 100*time.Millisecond)
		permit, err := p2.effects.Prepare(ctx, c, req)
		require.NoError(t, err)
		require.False(t, permit.Permitted, "the persisted lease expired before the permit could be consumed")
		require.Equal(t, domain.EffectDenied, permit.Effect.State)
		require.Equal(t, domain.DenyStaleExecution, permit.DenialCode)
	})

	t.Run("a lease that runs out while the reentry waits for the locks is checked as of the decision", func(t *testing.T) {
		t.Cleanup(func() { inv.ackLost.Store(false) })
		op, at := funded(t, p1.dispatchProcess, "eff_contend", 0)
		req := effectRequest(op, at, "activity-worker-1", 1)
		req.LeaseExpiresAt = time.Now().Add(1500 * time.Millisecond)
		c := effectCmd("contend", "body")
		inv.ackLost.Store(true)
		_, err := p1.effects.Prepare(ctx, c, req)
		require.ErrorIs(t, err, domain.ErrEffectUncertain)
		inv.ackLost.Store(false)
		// Another transaction holds the operation row while the lease runs
		// out; the reentry's consuming transaction waits behind it.
		tx, err := p1.pool.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "SELECT 1 FROM operations WHERE operation_id = $1 FOR UPDATE", op.ID)
		require.NoError(t, err)
		type answer struct {
			permit application.Permit
			err    error
		}
		done := make(chan answer, 1)
		go func() {
			permit, err := p2.effects.Prepare(ctx, c, req)
			done <- answer{permit, err}
		}()
		time.Sleep(time.Until(req.LeaseExpiresAt) + 200*time.Millisecond)
		select {
		case <-done:
			t.Fatal("the reentry decided while the operation row was locked")
		default:
		}
		require.NoError(t, tx.Rollback(ctx))
		got := <-done
		require.NoError(t, got.err)
		require.False(t, got.permit.Permitted, "the clock is read after the locks, not before the wait")
		require.Equal(t, domain.DenyStaleExecution, got.permit.DenialCode)
	})

	t.Run("a request denied for a stale epoch reenters its own denial; a changed field still conflicts", func(t *testing.T) {
		op, at := funded(t, p1.dispatchProcess, "eff_stale", 0)
		req := effectRequest(op, at, "activity-worker-1", 1)
		req.ExecutionEpoch = op.ExecutionEpoch + 1
		c := effectCmd("stale", "body")
		permit, err := p1.effects.Prepare(ctx, c, req)
		require.NoError(t, err)
		require.Equal(t, domain.EffectDenied, permit.Effect.State)
		require.Equal(t, domain.DenyStaleExecution, permit.DenialCode)
		again, err := p2.effects.Prepare(ctx, c, req)
		require.NoError(t, err, "the identical request returns the original denial")
		require.Equal(t, permit.Effect.ID, again.Effect.ID)
		require.False(t, again.Permitted)
		require.Equal(t, domain.DenyStaleExecution, again.DenialCode)
		changed := req
		changed.CanonicalSubject = "component/c1/page/p9"
		_, err = p1.effects.Prepare(ctx, c, changed)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "an immutable field changed under the same command")
		// The same rule for a send admission refused for a stale epoch.
		stale := modelRequest(op, at, "stale-call", "proxy-1", 10_000)
		stale.ExecutionEpoch = op.ExecutionEpoch + 1
		denied, err := p1.dispatch.Admit(ctx, admitCmd("stale-call", "m"), stale)
		require.NoError(t, err)
		require.False(t, denied.Allowed)
		require.Equal(t, domain.DenyStaleExecution, denied.DenialCode)
		same, err := p2.dispatch.Admit(ctx, admitCmd("stale-call", "m"), stale)
		require.NoError(t, err, "the identical admission returns its original denial")
		require.Equal(t, denied.Dispatch.ID, same.Dispatch.ID)
		require.False(t, same.Allowed)
	})
}

// hookedDisposition runs a hook once after the first successful evidence
// verification: the moment between the unlocked checks of a disposition and
// its commit, where the operation's recovery epoch can change.
type hookedDisposition struct {
	inner application.DispositionEvidence
	hook  func()
	once  sync.Once
}

func (h *hookedDisposition) VerifyDisposition(ctx context.Context, class, obligationID string, decision domain.DispositionDecision, evidenceRef string, evidenceDigest domain.Digest) error {
	if err := h.inner.VerifyDisposition(ctx, class, obligationID, decision, evidenceRef, evidenceDigest); err != nil {
		return err
	}
	if h.hook != nil {
		h.once.Do(h.hook)
	}
	return nil
}

// TestDisposeIsOneCommit: the epoch check, the obligation's change, the
// disposition row and the finding share one commit. A recovery epoch that
// moves between the evidence verification and the commit refuses the
// disposition with no partial change; a stale epoch refuses a not-sent
// confirmation before the dispatch changes.
func TestDisposeIsOneCommit(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(t.TempDir())
	require.NoError(t, err)
	inv := &hookedInventory{inner: fsInv}
	auth := &authorityDouble{allowed: map[string]bool{"tenant_a/" + modelRoute: true}}
	prices := newPriceSource(fixturePrices(t, "fx-model-d", 1))
	base := newDispatchProcess(t, inst, inv, auth, prices)
	store := postgres.NewStore(base.pool)
	queries := development.NewOutcomeQuery(inv, []string{issuer})
	effects := application.NewEffects(store, inv, queries, domain.SystemClock{}, testLog)
	evidence := &hookedDisposition{inner: development.NewDispositionEvidence(inv, []string{issuer})}
	recovery := application.NewRecovery(store, inv, queries, base.dispatch, effects, base.exec, []domain.Profile{profile}, auth,
		development.NewNotSentEvidence(inv, []string{issuer}), evidence, domain.SystemClock{}, testLog, 2)
	ctx := context.Background()
	pools(t, base.pool, "onecommit", [3]int64{1_000_000_000, 100_000_000, 10_000_000})
	opCmd := func(id string) domain.CommandIdentity {
		return domain.CommandIdentity{TenantID: "tenant_a", CommandID: id, ActorID: operatorActor, RequestDigest: application.DigestOf([]byte(id))}
	}

	op, at := funded(t, base, "one_e", 0)
	permit, err := effects.Prepare(ctx, effectCmd("one", "body"), effectRequest(op, at, "activity-worker-1", 1))
	require.NoError(t, err)
	require.True(t, permit.Permitted)
	eff := permit.Effect
	_, _, err = effects.Observe(ctx, "tenant_a", eff.ID, "worker", 1, domain.EffectOutcomeUnknown, "", "", time.Now())
	require.NoError(t, err)
	opS, atS := funded(t, base, "one_s", 50_000)
	sent, err := base.dispatch.Admit(ctx, admitCmd("one_s", "m"), modelRequest(opS, atS, "one_s", "proxy-1", 10_000))
	require.NoError(t, err)
	require.True(t, sent.Allowed)
	_, _, err = base.dispatch.Observe(ctx, sent.Dispatch.ID, "proxy-1", 1, domain.DispatchOutcomeUnknown, nil, "", time.Now())
	require.NoError(t, err)

	run, _, err := recovery.Begin(ctx, opCmd("one_begin"), "tenant_a", time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 5*time.Second, "one commit")
	require.NoError(t, err)
	for _, class := range application.ObligationClasses {
		for {
			progress, err := recovery.Enumerate(ctx, run.ID, class)
			require.NoError(t, err)
			if progress.Complete {
				break
			}
		}
	}
	findingOf := func(class, id string) *domain.RecoveryFinding {
		fs, _, _, err := recovery.Findings(ctx, run.ID, "", "", 100)
		require.NoError(t, err)
		for _, f := range fs {
			if f.Class == class && f.ObligationID == id {
				return f
			}
		}
		t.Fatalf("no finding for %s/%s", class, id)
		return nil
	}
	require.Equal(t, domain.FindingUnresolved, findingOf("business-write", eff.ID).Status)
	require.Equal(t, domain.FindingUnresolved, findingOf("model-dispatch", sent.Dispatch.ID).Status)

	key := development.DispositionKey("business-write", eff.ID)
	digest := publish(t, inv, key, development.DispositionAttestation{Class: "disposition", ObligationClass: "business-write", ObligationID: eff.ID, Decision: "resolve_succeeded", Issuer: issuer, SealedAt: time.Now().UTC().Format(time.RFC3339)})
	req := application.DispositionRequest{Class: "business-write", ObligationID: eff.ID, Decision: domain.DisposeResolveSucceeded, EvidenceRef: key, EvidenceDigest: digest, RecoveryEpoch: run.RecoveryEpoch, RunID: run.ID, Reason: "upstream console shows the write"}
	// Between the evidence verification and the commit a second run moves
	// the operation to a new recovery epoch.
	evidence.hook = func() {
		_, _, err := recovery.Begin(ctx, opCmd("one_begin_2"), "tenant_a", time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 5*time.Second, "second run")
		require.NoError(t, err)
	}
	_, _, err = recovery.Dispose(ctx, opCmd("one_race"), req)
	require.ErrorIs(t, err, domain.ErrStaleExecution)
	unchanged, err := effects.Get(ctx, "tenant_a", eff.ID, "", "", 0)
	require.NoError(t, err)
	require.Equal(t, domain.EffectUnknown, unchanged.State, "the refused disposition changed nothing")
	var observations, dispositions int
	require.NoError(t, base.pool.QueryRow(ctx, "SELECT count(*) FROM effect_observations WHERE effect_id = $1 AND source = 'disposition'", eff.ID).Scan(&observations))
	require.Zero(t, observations)
	require.NoError(t, base.pool.QueryRow(ctx, "SELECT count(*) FROM obligation_dispositions WHERE obligation_id = $1", eff.ID).Scan(&dispositions))
	require.Zero(t, dispositions)
	require.Equal(t, domain.FindingUnresolved, findingOf("business-write", eff.ID).Status)
	moved, err := base.ops.Get(ctx, scopeA, op.ID)
	require.NoError(t, err)
	require.Equal(t, run.RecoveryEpoch+1, moved.RecoveryEpoch)

	// Under the current epoch the same decision applies once, atomically.
	req.RecoveryEpoch = moved.RecoveryEpoch
	d, existing, err := recovery.Dispose(ctx, opCmd("one_ok"), req)
	require.NoError(t, err)
	require.False(t, existing)
	resolved, err := effects.Get(ctx, "tenant_a", eff.ID, "", "", 0)
	require.NoError(t, err)
	require.Equal(t, domain.EffectSucceeded, resolved.State)
	require.NoError(t, base.pool.QueryRow(ctx, "SELECT count(*) FROM obligation_dispositions WHERE disposition_id = $1", d.ID).Scan(&dispositions))
	require.Equal(t, 1, dispositions)
	require.Equal(t, domain.FindingDisposed, findingOf("business-write", eff.ID).Status)
	same, existing, err := recovery.Dispose(ctx, opCmd("one_ok"), req)
	require.NoError(t, err)
	require.True(t, existing)
	require.Equal(t, d.ID, same.ID, "the repeated command returns the original disposition")

	// A not-sent confirmation under a stale epoch refuses before the
	// dispatch changes: the exposure stays reserved and the state unknown.
	ref, notSent := attestation(t, inv, sent.Dispatch, issuer)
	_, _, err = recovery.Dispose(ctx, opCmd("one_ns_stale"), application.DispositionRequest{Class: "model-dispatch", ObligationID: sent.Dispatch.ID, Decision: domain.DisposeConfirmNotSent, EvidenceRef: ref, EvidenceDigest: notSent, RecoveryEpoch: run.RecoveryEpoch, RunID: run.ID})
	require.ErrorIs(t, err, domain.ErrStaleExecution)
	still, err := base.dispatch.Get(ctx, sent.Dispatch.ID, "", "")
	require.NoError(t, err)
	require.Equal(t, domain.DispatchUnknown, still.State)
	require.Equal(t, int64(10_000), still.Reserved.Amount)
	movedS, err := base.ops.Get(ctx, scopeA, opS.ID)
	require.NoError(t, err)
	_, _, err = recovery.Dispose(ctx, opCmd("one_ns"), application.DispositionRequest{Class: "model-dispatch", ObligationID: sent.Dispatch.ID, Decision: domain.DisposeConfirmNotSent, EvidenceRef: ref, EvidenceDigest: notSent, RecoveryEpoch: movedS.RecoveryEpoch, RunID: run.ID})
	require.NoError(t, err)
	released, err := base.dispatch.Get(ctx, sent.Dispatch.ID, "", "")
	require.NoError(t, err)
	require.Equal(t, domain.DispatchConfirmedNotSent, released.State)
	require.Equal(t, domain.FindingDisposed, findingOf("model-dispatch", sent.Dispatch.ID).Status)
}

// TestFindingsPagination: a run with more findings than one page (and more
// than the 1,000 a single listing can return) is walked completely from
// the cursor; every finding, whatever its state and position, is reached.
func TestFindingsPagination(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(t.TempDir())
	require.NoError(t, err)
	inv := &hookedInventory{inner: fsInv}
	auth := &authorityDouble{allowed: map[string]bool{}}
	p := newP07Process(t, inst, inv, auth, newPriceSource(fixturePrices(t, "fx-model-p", 1)))
	ctx := context.Background()
	operator := domain.CommandIdentity{TenantID: "tenant_a", CommandID: "page_begin", ActorID: operatorActor, RequestDigest: application.DigestOf([]byte("page"))}
	run, _, err := p.recovery.Begin(ctx, operator, "tenant_a", time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 0, "pagination")
	require.NoError(t, err)
	const total = 1050
	now := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, postgres.NewStore(p.pool).Tx(ctx, func(r application.Repo) error {
		for i := range total {
			id := fmt.Sprintf("dsp_%05d", i)
			status := domain.FindingUnresolved
			if i%7 == 0 {
				status = domain.FindingPresent
			}
			if err := r.InsertFinding(ctx, &domain.RecoveryFinding{
				ID: "fnd_" + id, RunID: run.ID, Class: application.ClassModelDispatch, ObligationID: id, TenantID: "tenant_a", InventoryKey: application.ObligationKey(application.ClassModelDispatch, id),
				InventoryVersion: "v1", RecordedAt: now, Status: status, CreatedAt: now, UpdatedAt: now,
			}); err != nil {
				return err
			}
		}
		return nil
	}))
	seen := map[string]bool{}
	pages, cursor := 0, ""
	for {
		fs, next, complete, err := p.recovery.Findings(ctx, run.ID, "", cursor, 200)
		require.NoError(t, err)
		pages++
		for _, f := range fs {
			require.False(t, seen[f.ObligationID], "no finding is listed twice")
			seen[f.ObligationID] = true
		}
		if complete {
			require.Empty(t, next)
			break
		}
		require.NotEmpty(t, next)
		cursor = next
	}
	require.Len(t, seen, total, "every finding is reached, the ones beyond the first thousand included")
	require.True(t, seen["dsp_01049"])
	require.Equal(t, 6, pages)
	unresolvedOnly := map[string]bool{}
	cursor = ""
	for {
		fs, next, complete, err := p.recovery.Findings(ctx, run.ID, domain.FindingUnresolved, cursor, 1000)
		require.NoError(t, err)
		for _, f := range fs {
			require.Equal(t, domain.FindingUnresolved, f.Status)
			unresolvedOnly[f.ObligationID] = true
		}
		if complete {
			break
		}
		cursor = next
	}
	present := 0
	for i := range total {
		if i%7 == 0 {
			present++
		}
	}
	require.Len(t, unresolvedOnly, total-present, "a status filter pages the same way")
	_, _, _, err = p.recovery.Findings(ctx, run.ID, "", "not-a-cursor", 10)
	require.ErrorIs(t, err, domain.ErrInvalid)
	unsettled, err := func() (uint64, error) { _, n, err := p.recovery.Evaluate(ctx, run.ID); return n, err }()
	require.NoError(t, err)
	require.Positive(t, unsettled)
}

// TestRecovery runs the ordered reconciliation of a rollback window on two
// Control replicas over one database and one inventory: obligations of
// every class are recorded, the database then loses some of them (rows
// removed as the superuser, which is what a restore to an earlier point
// leaves behind), and the run must close admission, fence, enumerate
// completely, restore the lost identities, record original outcomes and
// reopen only when everything is settled.
func TestRecovery(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(t.TempDir())
	require.NoError(t, err)
	inv := &hookedInventory{inner: fsInv}
	auth := &authorityDouble{allowed: map[string]bool{"tenant_a/" + modelRoute: true}}
	prices := newPriceSource(fixturePrices(t, "fx-model-r", 1))
	p1, p2 := newP07Process(t, inst, inv, auth, prices), newP07Process(t, inst, inv, auth, prices)
	ctx := context.Background()
	pools(t, p1.pool, "recovery", [3]int64{1_000_000_000, 100_000_000, 10_000_000})
	db := admin(t, inst)
	operator := domain.CommandIdentity{TenantID: "tenant_a", CommandID: "rec_begin", ActorID: operatorActor, RequestDigest: application.DigestOf([]byte("begin"))}

	// Obligations of every class in tenant_a, plus one in tenant_b.
	opA, atA := funded(t, p1.dispatchProcess, "rec_a", 50_000)
	reqA := modelRequest(opA, atA, "rec_a", "proxy-1", 10_000)
	admitted, err := p1.dispatch.Admit(ctx, admitCmd("rec_a", "m"), reqA)
	require.NoError(t, err)
	require.True(t, admitted.Allowed)
	dispatchA := admitted.Dispatch
	opB, atB := funded(t, p1.dispatchProcess, "rec_b", 0)
	permit, err := p1.effects.Prepare(ctx, effectCmd("rec_b", "body"), effectRequest(opB, atB, "activity-worker-1", 1))
	require.NoError(t, err)
	require.True(t, permit.Permitted)
	effectB := permit.Effect
	opC, atC := funded(t, p1.dispatchProcess, "rec_c", 0)
	launchC, _, err := p1.exec.PrepareLaunch(ctx, cmd("tenant_a", "rec_c_launch", "l"), atC.ID, "lc-rec-c", "kind", imageDig, time.Now().Add(time.Minute))
	require.NoError(t, err)
	opE, _, err := p1.ops.Create(ctx, cmd("tenant_a", "rec_e", "body"), scopeA, domain.KindLocalCheck, subject, nil)
	require.NoError(t, err)
	opF, _ := funded(t, p1.dispatchProcess, "rec_f", 0) // stays present
	opG, atG := funded(t, p1.dispatchProcess, "rec_g", 50_000)
	g, err := p1.dispatch.Admit(ctx, admitCmd("rec_g", "m"), modelRequest(opG, atG, "rec_g", "proxy-1", 10_000))
	require.NoError(t, err)
	require.True(t, g.Allowed)
	_, _, err = p1.dispatch.Observe(ctx, g.Dispatch.ID, "proxy-1", 1, domain.DispatchOutcomeUnknown, nil, "", time.Now())
	require.NoError(t, err, "G's send has an unknown outcome and stays present in the database")
	opOther, _, err := p1.ops.Create(ctx, cmd("tenant_b", "rec_other", "body"), scopeB, domain.KindLocalCheck, subject, nil)
	require.NoError(t, err)
	// D's launch and B2's permitted write stay present in the database with
	// nothing settled about their external effect; the malformed intake
	// object lacks its tenant binding.
	_, atD := funded(t, p1.dispatchProcess, "rec_d", 0)
	launchD, _, err := p1.exec.PrepareLaunch(ctx, cmd("tenant_a", "rec_d_launch", "l"), atD.ID, "lc-rec-d", "kind", imageDig, time.Now().Add(time.Minute))
	require.NoError(t, err)
	opB2, atB2 := funded(t, p1.dispatchProcess, "rec_b2", 0)
	permitB2, err := p1.effects.Prepare(ctx, effectCmd("rec_b2", "body"), effectRequest(opB2, atB2, "activity-worker-1", 1))
	require.NoError(t, err)
	require.True(t, permitB2.Permitted)
	effectB2 := permitB2.Effect
	const malformedID = "op_malformed"
	publish(t, inv, application.ObligationKey(application.ClassIntake, malformedID), map[string]any{
		"class": "intake", "operationId": malformedID, "commandId": "cmd_malformed", "kind": "local_check", "profileId": "local-check-v1",
		"subjectDigest": string(subject.SubjectDigest), "requestDigest": string(subject.SubjectDigest), "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "executionEpoch": 1,
	})

	// The rollback: the database loses A's dispatch, B's effect, C's
	// launch and E entirely; the inventory keeps every obligation.
	for _, stmt := range []string{
		"DELETE FROM cost_entries WHERE dispatch_id = '" + dispatchA.ID + "'",
		"DELETE FROM dispatches WHERE dispatch_id = '" + dispatchA.ID + "'",
		"DELETE FROM effect_intents WHERE effect_id = '" + effectB.ID + "'",
		"DELETE FROM launches WHERE launch_id = '" + launchC.ID + "'",
		"DELETE FROM operation_events WHERE operation_id = '" + opE.ID + "'",
		"DELETE FROM operations WHERE operation_id = '" + opE.ID + "'",
	} {
		_, err := db.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	var run *domain.RecoveryRun
	t.Run("begin closes admission, fences active identities and establishes the epoch in one commit", func(t *testing.T) {
		_, _, err := p1.recovery.Begin(ctx, domain.CommandIdentity{TenantID: "tenant_a", CommandID: "rec_nope", ActorID: "user_a", RequestDigest: operator.RequestDigest}, "tenant_a", time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 5*time.Second, "test")
		require.ErrorIs(t, err, domain.ErrForbidden, "only an authorized operator begins a run")
		var existing bool
		run, existing, err = p1.recovery.Begin(ctx, operator, "tenant_a", time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 5*time.Second, "PITR drill")
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.RecoveryFenced, run.Phase)
		require.Equal(t, uint64(1), run.RecoveryEpoch)
		again, existing, err := p2.recovery.Begin(ctx, operator, "tenant_a", time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 5*time.Second, "PITR drill")
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, run.ID, again.ID)

		fenced, err := p2.ops.Get(ctx, scopeA, opA.ID)
		require.NoError(t, err)
		require.Equal(t, opA.ExecutionEpoch+1, fenced.ExecutionEpoch, "old execution identities are retired")
		require.Equal(t, run.RecoveryEpoch, fenced.RecoveryEpoch)
		require.Equal(t, domain.LifecycleReconciling, fenced.Lifecycle)
		_, _, err = p2.ops.Create(ctx, cmd("tenant_a", "rec_closed", "body"), scopeA, domain.KindLocalCheck, subject, nil)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "new intake is closed for the scope")
		require.ErrorContains(t, err, domain.DenyRecoveryRestricted)
		denied, err := p2.dispatch.Admit(ctx, admitCmd("rec_closed", "m"), modelRequest(fenced, atA, "rec_closed", "proxy-1", 10_000))
		require.NoError(t, err)
		require.False(t, denied.Allowed)
		require.Equal(t, domain.DenyRecoveryRestricted, denied.DenialCode, "no send permission while the scope is closed")
		_, _, err = p2.exec.OpenAttempt(ctx, cmd("tenant_a", "rec_closed_open", "o"), opF.ID, "local-check", 1, "local-check-v1")
		require.ErrorIs(t, err, domain.ErrStaleExecution)
		other, _, err := p1.ops.Create(ctx, cmd("tenant_b", "rec_open_b", "body"), scopeB, domain.KindLocalCheck, subject, nil)
		require.NoError(t, err, "another tenant's scope is untouched")
		require.Equal(t, uint64(1), other.RecoveryEpoch)
		unchanged, err := p1.ops.Get(ctx, scopeB, opOther.ID)
		require.NoError(t, err)
		require.Equal(t, uint64(1), unchanged.ExecutionEpoch)

		wf := &fakeWorkflows{starts: map[string]int{}, cancels: map[string]int{}}
		relay := application.NewRelay(postgres.NewStore(p1.pool), wf, p1.ops, p1.recovery, application.NewPreparations(postgres.NewStore(p1.pool), []domain.Profile{profile}, domain.SystemClock{}, testLog), application.NewGenerations(postgres.NewStore(p1.pool), nil, []domain.Profile{profile}, nil, domain.SystemClock{}, testLog), domain.SystemClock{}, testLog, time.Second)
		require.NoError(t, relay.Tick(ctx))
		require.NoError(t, relay.Tick(ctx))
		require.Equal(t, 1, wf.starts["recovery:"+run.ID], "the relay starts the reconciliation workflow once")
		current, _, _, err := p1.recovery.Get(ctx, run.ID)
		require.NoError(t, err)
		require.Equal(t, domain.RecoveryEnumerating, current.Phase)
	})

	t.Run("an interrupted listing keeps the cursor and is never an empty inventory", func(t *testing.T) {
		t.Cleanup(func() { inv.failing.Store(false) })
		inv.failing.Store(true)
		_, err := p1.recovery.Enumerate(ctx, run.ID, application.ClassIntake)
		require.ErrorIs(t, err, domain.ErrEffectUncertain)
		inv.failing.Store(false)
		current, progress, _, err := p1.recovery.Get(ctx, run.ID)
		require.NoError(t, err)
		require.Equal(t, domain.RecoveryEnumerating, current.Phase)
		for _, p := range progress {
			require.False(t, p.Complete)
			require.Empty(t, p.Cursor)
		}
		evaluated, unsettled, err := p2.recovery.Evaluate(ctx, run.ID)
		require.NoError(t, err)
		require.Equal(t, domain.RecoveryEnumerating, evaluated.Phase, "an incomplete enumeration never reopens")
		require.Positive(t, unsettled)
	})

	t.Run("two replicas enumerate every class completely without duplicate findings", func(t *testing.T) {
		var wg sync.WaitGroup
		for _, p := range []*p07Process{p1, p2} {
			wg.Add(1)
			go func(p *p07Process) {
				defer wg.Done()
				for _, class := range application.ObligationClasses {
					for {
						progress, err := p.recovery.Enumerate(ctx, run.ID, class)
						require.NoError(t, err)
						if progress.Complete {
							break
						}
					}
				}
			}(p)
		}
		wg.Wait()
		current, progress, _, err := p1.recovery.Get(ctx, run.ID)
		require.NoError(t, err)
		require.Equal(t, domain.RecoveryReconciling, current.Phase)
		require.Len(t, progress, 5)
		for _, p := range progress {
			require.True(t, p.Complete, p.Class)
		}
		findings, _, complete, err := p2.recovery.Findings(ctx, run.ID, "", "", 100)
		require.NoError(t, err)
		require.True(t, complete)
		byKey := map[string]domain.FindingStatus{}
		for _, f := range findings {
			require.NotContains(t, byKey, f.Class+"/"+f.ObligationID, "one finding per obligation across replicas")
			byKey[f.Class+"/"+f.ObligationID] = f.Status
		}
		require.Equal(t, domain.FindingMissing, byKey["intake/"+opE.ID])
		require.Equal(t, domain.FindingMissing, byKey["job-launch/"+launchC.ID])
		require.Equal(t, domain.FindingMissing, byKey["model-dispatch/"+dispatchA.ID])
		require.Equal(t, domain.FindingMissing, byKey["business-write/"+effectB.ID])
		require.Equal(t, domain.FindingPresent, byKey["intake/"+opA.ID])
		require.Equal(t, domain.FindingPresent, byKey["intake/"+opF.ID])
		require.NotContains(t, byKey, "intake/"+opOther.ID, "another tenant's obligations are outside the scope")
		require.Equal(t, 4, countStatus(findings, domain.FindingMissing))
		// Present in the database is not settled: an unknown send, a
		// permitted write without an outcome and a launch without confirmed
		// cleanup keep the scope restricted exactly like a lost identity.
		require.Equal(t, domain.FindingUnresolved, byKey["model-dispatch/"+g.Dispatch.ID], "an unknown outcome present in the database is not settled")
		require.Equal(t, domain.FindingUnresolved, byKey["business-write/"+effectB2.ID], "a permitted write without an observed outcome is not settled")
		require.Equal(t, domain.FindingUnresolved, byKey["job-launch/"+launchD.ID], "a launch without confirmed cleanup is not settled")
		require.Equal(t, domain.FindingUnresolved, byKey["intake/"+malformedID], "an object lacking its tenant binding is an integrity finding, never silently out of scope")
	})

	findingOf := func(class, id string) *domain.RecoveryFinding {
		findings, _, _, err := p1.recovery.Findings(ctx, run.ID, "", "", 100)
		require.NoError(t, err)
		for _, f := range findings {
			if f.Class == class && f.ObligationID == id {
				return f
			}
		}
		t.Fatalf("no finding for %s/%s", class, id)
		return nil
	}

	t.Run("a lost intake is restored fenced under the run's epoch", func(t *testing.T) {
		f, err := p1.recovery.Reconcile(ctx, run.ID, findingOf("intake", opE.ID).ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingResolved, f.Status)
		restored, err := p2.ops.Get(ctx, scopeA, opE.ID)
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleReconciling, restored.Lifecycle)
		require.Equal(t, opE.ExecutionEpoch+1, restored.ExecutionEpoch)
		require.Equal(t, run.RecoveryEpoch, restored.RecoveryEpoch)
		require.Equal(t, domain.IntakeConfirmed, restored.Intake)
		require.Equal(t, opE.CommandID, restored.CommandID, "the original identity is kept")
		require.Equal(t, "RECOVERED_FROM_INVENTORY", restored.FailureCode)
		_, _, err = p2.exec.OpenAttempt(ctx, cmd("tenant_a", "rec_e_open", "o"), opE.ID, "local-check", 0, "local-check-v1")
		require.ErrorIs(t, err, domain.ErrStaleExecution, "nothing is admitted for a restored operation")
	})

	t.Run("present obligations settle only on evidence: a definite cleanup, an observed outcome, or a disposition", func(t *testing.T) {
		// D: reconciling the present launch changes nothing until its
		// attempt is closed with definite cleanup by its original owner.
		fD, err := p1.recovery.Reconcile(ctx, run.ID, findingOf("job-launch", launchD.ID).ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingUnresolved, fD.Status)
		require.Contains(t, fD.Detail, "cleanup not confirmed")
		_, _, _, err = p2.exec.CloseAttempt(ctx, cmd("tenant_a", "rec_d_close", "c"), atD.ID, domain.OutcomeCanceled, domain.CleanupComplete, "CANCELED")
		require.NoError(t, err)
		fD, err = p2.recovery.Reconcile(ctx, run.ID, fD.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingResolved, fD.Status)
		require.Equal(t, "cleanup_confirmed", fD.Outcome)
		// B2: the permitted write stays unresolved until its outcome is
		// observed under the original identity.
		fB2, err := p2.recovery.Reconcile(ctx, run.ID, findingOf("business-write", effectB2.ID).ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingUnresolved, fB2.Status, "no upstream record and no observation yet")
		_, _, err = p1.effects.Observe(ctx, "tenant_a", effectB2.ID, "worker", 1, domain.EffectOutcomeSucceeded, "rcpt-b2", "", time.Now())
		require.NoError(t, err)
		fB2, err = p1.recovery.Reconcile(ctx, run.ID, fB2.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingResolved, fB2.Status)
		// The malformed object cannot be restored as an intake and has no
		// operation to bind a decision to: it is settled only by an
		// operator retaining its exposure against the run's epoch.
		fM, err := p1.recovery.Reconcile(ctx, run.ID, findingOf("intake", malformedID).ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingUnresolved, fM.Status)
		require.Contains(t, fM.Detail, "tenantId")
		key := development.DispositionKey("intake", malformedID)
		digest := publish(t, inv, key, development.DispositionAttestation{Class: "disposition", ObligationClass: "intake", ObligationID: malformedID, Decision: "retain_exposure", Issuer: issuer, SealedAt: time.Now().UTC().Format(time.RFC3339)})
		opCmd := domain.CommandIdentity{TenantID: "tenant_a", CommandID: "dsp_malformed", ActorID: operatorActor, RequestDigest: application.DigestOf([]byte("m"))}
		_, _, err = p1.recovery.Dispose(ctx, opCmd, application.DispositionRequest{Class: "intake", ObligationID: malformedID, Decision: domain.DisposeRetainExposure, EvidenceRef: key, EvidenceDigest: digest, RecoveryEpoch: run.RecoveryEpoch, Reason: "unreadable object"})
		require.ErrorIs(t, err, domain.ErrNotFound, "without the run there is nothing to bind the decision to")
		d, _, err := p2.recovery.Dispose(ctx, opCmd, application.DispositionRequest{Class: "intake", ObligationID: malformedID, Decision: domain.DisposeRetainExposure, EvidenceRef: key, EvidenceDigest: digest, RecoveryEpoch: run.RecoveryEpoch, RunID: run.ID, Reason: "unreadable object"})
		require.NoError(t, err)
		require.Equal(t, domain.DisposeRetainExposure, d.Decision)
		require.Equal(t, domain.FindingDisposed, findingOf("intake", malformedID).Status)
	})

	t.Run("a lost dispatch is restored unknown with its exposure retained and resolved only by a trusted original-identity answer", func(t *testing.T) {
		f, err := p2.recovery.Reconcile(ctx, run.ID, findingOf("model-dispatch", dispatchA.ID).ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingUnresolved, f.Status, "no upstream record yet")
		restored, err := p1.dispatch.Get(ctx, dispatchA.ID, "", "")
		require.NoError(t, err)
		require.Equal(t, domain.DispatchUnknown, restored.State)
		require.Equal(t, dispatchA.CallID, restored.CallID)
		require.Equal(t, int64(10_000), restored.Reserved.Amount, "unknown keeps its unresolved exposure, never zero")
		op, err := p1.ops.Get(ctx, scopeA, opA.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FinanceExposureUnknown, op.Finance)
		stale := reqA
		stale.ExecutionEpoch = opA.ExecutionEpoch + 1
		_, err = p1.dispatch.Admit(ctx, admitCmd("rec_a", "m"), stale)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "the original call cannot borrow the new epoch")
		again, err := p1.dispatch.Admit(ctx, admitCmd("rec_a", "m"), reqA)
		require.NoError(t, err)
		require.False(t, again.Allowed, "the original call never regains permission")
		publish(t, inv, development.OutcomeKey("model-dispatch", dispatchA.ID), development.OutcomeAttestation{
			Class: "outcome", ObligationClass: "model-dispatch", ObligationID: dispatchA.ID, Issuer: issuer, Sequence: 1, Outcome: "succeeded",
			Usage: map[string]uint64{"input_units": 1000, "output_units": 100}, ObservedAt: time.Now().UTC().Format(time.RFC3339),
		})
		f, err = p1.recovery.Reconcile(ctx, run.ID, f.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingResolved, f.Status)
		require.Equal(t, "succeeded", f.Outcome)
		observed, err := p2.dispatch.Get(ctx, dispatchA.ID, "", "")
		require.NoError(t, err)
		require.Equal(t, domain.DispatchObserved, observed.State)
		kinds, total := entries(t, p1.pool, dispatchA.ID)
		require.Equal(t, []string{"actual"}, kinds, "the metered usage is charged under the frozen revision")
		require.Positive(t, total)
		f, err = p2.recovery.Reconcile(ctx, run.ID, f.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingResolved, f.Status, "reconciling a settled finding changes nothing")
	})

	t.Run("a lost business write is restored unknown and a late outcome settles it once", func(t *testing.T) {
		f, err := p1.recovery.Reconcile(ctx, run.ID, findingOf("business-write", effectB.ID).ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingUnresolved, f.Status)
		restored, err := p2.effects.Get(ctx, "tenant_a", effectB.ID, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, domain.EffectUnknown, restored.State)
		require.Equal(t, effectB.CanonicalSubject, restored.CanonicalSubject)
		require.Equal(t, run.RecoveryEpoch, restored.RecoveryEpoch)
		// The sender's own late report arrives after the run queried.
		late, _, err := p2.effects.Observe(ctx, "tenant_a", effectB.ID, "worker", 1, domain.EffectOutcomeSucceeded, "rcpt-late", "", time.Now())
		require.NoError(t, err)
		require.Equal(t, domain.EffectSucceeded, late.State)
		f, err = p1.recovery.Reconcile(ctx, run.ID, f.ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingResolved, f.Status)
		publish(t, inv, development.OutcomeKey("business-write", effectB.ID), development.OutcomeAttestation{Class: "outcome", ObligationClass: "business-write", ObligationID: effectB.ID, Issuer: issuer, Sequence: 2, Outcome: "failed", ReceiptRef: "rcpt-x", ObservedAt: time.Now().UTC().Format(time.RFC3339)})
		_, _, err = p1.effects.QueryOriginal(ctx, "tenant_a", effectB.ID)
		require.NoError(t, err)
		settled, err := p1.effects.Get(ctx, "tenant_a", effectB.ID, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, domain.EffectSucceeded, settled.State, "a later, different upstream answer never overwrites the first definite outcome")
	})

	t.Run("a lost launch is restored and settled by the launcher's evidence; duplicate observations register one instance", func(t *testing.T) {
		f, err := p2.recovery.Reconcile(ctx, run.ID, findingOf("job-launch", launchC.ID).ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingRestored, f.Status)
		listed, _, _, err := p1.recovery.Findings(ctx, run.ID, domain.FindingRestored, "", 10)
		require.NoError(t, err)
		require.Len(t, listed, 1)
		require.Equal(t, "lc-rec-c", listed[0].LaunchKey, "the launcher needs the original launch key")
		// A stop reported before any observation was recorded proves nothing
		// about what was stopped; absence within a settle window is not
		// evidence either. Neither closes the attempt.
		f, err = p1.recovery.RecordOutcome(ctx, run.ID, f.ID, application.LaunchOutcome{Stopped: true})
		require.NoError(t, err)
		require.Equal(t, domain.FindingUnresolved, f.Status)
		require.Contains(t, f.Detail, "without a recorded observation")
		f, err = p2.recovery.RecordOutcome(ctx, run.ID, f.ID, application.LaunchOutcome{})
		require.NoError(t, err)
		require.Equal(t, domain.FindingUnresolved, f.Status)
		require.Contains(t, f.Detail, "absence")
		var attemptState string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT state FROM attempts WHERE attempt_id = $1", atC.ID).Scan(&attemptState))
		require.NotEqual(t, "closed", attemptState, "nothing settled the attempt")
		// The observation is recorded before anything is deleted.
		f, err = p1.recovery.RecordOutcome(ctx, run.ID, f.ID, application.LaunchOutcome{JobUID: "job-c", Pods: []application.PodEvidence{{PodUID: "pod-c", Phase: domain.PhaseSucceeded, ObservedAt: time.Now()}}, Stopped: false})
		require.NoError(t, err)
		require.Equal(t, domain.FindingUnresolved, f.Status, "a Job still present is not a stopped writer")
		require.Equal(t, "observed", f.Outcome)
		require.Equal(t, "job:job-c", f.EvidenceRef, "the observed Job is the finding's evidence")
		var wg sync.WaitGroup
		for _, p := range []*p07Process{p1, p2} {
			wg.Add(1)
			go func(p *p07Process) {
				defer wg.Done()
				_, err := p.recovery.RecordOutcome(ctx, run.ID, f.ID, application.LaunchOutcome{JobUID: "job-c", Pods: []application.PodEvidence{{PodUID: "pod-c", Phase: domain.PhaseSucceeded, ObservedAt: time.Now()}}, Stopped: true})
				require.NoError(t, err)
			}(p)
		}
		wg.Wait()
		settled := findingOf("job-launch", launchC.ID)
		require.Equal(t, domain.FindingResolved, settled.Status)
		require.Equal(t, "job_stopped", settled.Outcome)
		var instances int
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT count(*) FROM physical_instances WHERE launch_key = 'lc-rec-c'").Scan(&instances))
		require.Equal(t, 1, instances, "duplicate launch observations register one physical instance")
		at, err := p1.exec.GetAcceptedStage(ctx, atC.ID, "", "")
		require.ErrorIs(t, err, domain.ErrNotFound, "a stopped Job proves no result")
		_ = at
		var state, outcome string
		require.NoError(t, p1.pool.QueryRow(ctx, "SELECT state, coalesce(outcome, '') FROM attempts WHERE attempt_id = $1", atC.ID).Scan(&state, &outcome))
		require.Equal(t, "closed", state)
		require.Equal(t, "unknown", outcome, "unknown never becomes success")
		closed, err := p1.ops.Get(ctx, scopeA, opC.ID)
		require.NoError(t, err)
		require.Equal(t, domain.LifecycleReconciling, closed.Lifecycle)
	})

	t.Run("operator dispositions validate authorization, scope, epoch and evidence, one obligation each", func(t *testing.T) {
		// G's dispatch is present in the database but its outcome is unknown
		// with no upstream record: the operator has to decide with evidence.
		unresolvedG, err := p2.recovery.Reconcile(ctx, run.ID, findingOf("model-dispatch", g.Dispatch.ID).ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingUnresolved, unresolvedG.Status, "the run queried the original identity and found no record")
		fenced, err := p1.ops.Get(ctx, scopeA, opG.ID)
		require.NoError(t, err)
		require.Equal(t, run.RecoveryEpoch, fenced.RecoveryEpoch)

		evidenceKey := development.DispositionKey("model-dispatch", g.Dispatch.ID)
		digest := publish(t, inv, evidenceKey, development.DispositionAttestation{Class: "disposition", ObligationClass: "model-dispatch", ObligationID: g.Dispatch.ID, Decision: "retain_exposure", Issuer: issuer, SealedAt: time.Now().UTC().Format(time.RFC3339)})
		req := application.DispositionRequest{Class: "model-dispatch", ObligationID: g.Dispatch.ID, Decision: domain.DisposeRetainExposure, EvidenceRef: evidenceKey, EvidenceDigest: digest, RecoveryEpoch: run.RecoveryEpoch, RunID: run.ID, Reason: "provider console shows no request"}
		opCmd := func(id string) domain.CommandIdentity {
			return domain.CommandIdentity{TenantID: "tenant_a", CommandID: id, ActorID: operatorActor, RequestDigest: application.DigestOf([]byte(id))}
		}
		_, _, err = p1.recovery.Dispose(ctx, domain.CommandIdentity{TenantID: "tenant_a", CommandID: "dsp_user", ActorID: "user_a", RequestDigest: application.DigestOf([]byte("x"))}, req)
		require.ErrorIs(t, err, domain.ErrForbidden)
		wrongEpoch := req
		wrongEpoch.RecoveryEpoch = run.RecoveryEpoch + 1
		_, _, err = p1.recovery.Dispose(ctx, opCmd("dsp_epoch"), wrongEpoch)
		require.ErrorIs(t, err, domain.ErrStaleExecution)
		wrongDigest := req
		wrongDigest.EvidenceDigest = application.DigestOf([]byte("forged"))
		_, _, err = p1.recovery.Dispose(ctx, opCmd("dsp_digest"), wrongDigest)
		require.ErrorIs(t, err, domain.ErrEvidenceInsufficient)
		_, _, err = p1.recovery.Dispose(ctx, domain.CommandIdentity{TenantID: "tenant_b", CommandID: "dsp_tenant", ActorID: operatorActor, RequestDigest: application.DigestOf([]byte("x"))}, req)
		require.ErrorIs(t, err, domain.ErrNotFound, "another tenant's obligation is not visible")
		wrongDecision := req
		wrongDecision.Decision = domain.DisposeResolveSucceeded
		_, _, err = p1.recovery.Dispose(ctx, opCmd("dsp_decision"), wrongDecision)
		require.ErrorIs(t, err, domain.ErrInvalid, "no decision turns an unknown send into a success")

		d, existing, err := p2.recovery.Dispose(ctx, opCmd("dsp_ok"), req)
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, domain.DisposeRetainExposure, d.Decision)
		same, existing, err := p1.recovery.Dispose(ctx, opCmd("dsp_ok"), req)
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, d.ID, same.ID)
		_, _, err = p1.recovery.Dispose(ctx, opCmd("dsp_twice"), req)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "one disposition per obligation")
		retained, err := p1.dispatch.Get(ctx, g.Dispatch.ID, "", "")
		require.NoError(t, err)
		require.Equal(t, domain.DispatchUnknown, retained.State, "the obligation keeps its identity and exposure")
		require.Equal(t, domain.FindingDisposed, findingOf("model-dispatch", g.Dispatch.ID).Status)
	})

	t.Run("the scope reopens only when every finding is settled, and stays reopened", func(t *testing.T) {
		evaluated, unsettled, err := p1.recovery.Evaluate(ctx, run.ID)
		require.NoError(t, err)
		require.Zero(t, unsettled)
		require.Equal(t, domain.RecoveryReopened, evaluated.Phase)
		require.NotNil(t, evaluated.ReopenedAt)
		_, _, err = p2.ops.Create(ctx, cmd("tenant_a", "rec_reopened", "body"), scopeA, domain.KindLocalCheck, subject, nil)
		require.NoError(t, err, "admission is open again")
		again, _, err := p2.recovery.Evaluate(ctx, run.ID)
		require.NoError(t, err)
		require.Equal(t, domain.RecoveryReopened, again.Phase)
	})

	t.Run("a run with an unresolved finding stays restricted", func(t *testing.T) {
		// Another run over the same scope, with a dispatch whose upstream
		// record is missing.
		opH, atH := funded(t, p1.dispatchProcess, "rec_h", 50_000)
		h, err := p1.dispatch.Admit(ctx, admitCmd("rec_h", "m"), modelRequest(opH, atH, "rec_h", "proxy-1", 10_000))
		require.NoError(t, err)
		require.True(t, h.Allowed)
		_, err = db.Exec(ctx, "DELETE FROM cost_entries WHERE dispatch_id = $1", h.Dispatch.ID)
		require.NoError(t, err)
		_, err = db.Exec(ctx, "DELETE FROM dispatches WHERE dispatch_id = $1", h.Dispatch.ID)
		require.NoError(t, err)
		second := domain.CommandIdentity{TenantID: "tenant_a", CommandID: "rec_begin_2", ActorID: operatorActor, RequestDigest: application.DigestOf([]byte("begin2"))}
		run2, _, err := p2.recovery.Begin(ctx, second, "tenant_a", time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 5*time.Second, "second drill")
		require.NoError(t, err)
		require.Equal(t, run.RecoveryEpoch+1, run2.RecoveryEpoch, "every run establishes a new epoch")
		for _, class := range application.ObligationClasses {
			for {
				progress, err := p1.recovery.Enumerate(ctx, run2.ID, class)
				require.NoError(t, err)
				if progress.Complete {
					break
				}
			}
		}
		missing, _, _, err := p1.recovery.Findings(ctx, run2.ID, domain.FindingMissing, "", 10)
		require.NoError(t, err)
		require.Len(t, missing, 1)
		require.Equal(t, h.Dispatch.ID, missing[0].ObligationID)
		disposed, _, _, err := p2.recovery.Findings(ctx, run2.ID, domain.FindingDisposed, "", 10)
		require.NoError(t, err)
		require.Len(t, disposed, 2, "the earlier dispositions of G's dispatch and the malformed object stand in the new run")
		f, err := p2.recovery.Reconcile(ctx, run2.ID, missing[0].ID)
		require.NoError(t, err)
		require.Equal(t, domain.FindingUnresolved, f.Status)
		evaluated, unsettled, err := p1.recovery.Evaluate(ctx, run2.ID)
		require.NoError(t, err)
		require.Equal(t, domain.RecoveryRestricted, evaluated.Phase)
		require.Equal(t, uint64(1), unsettled)
		_, _, err = p2.ops.Create(ctx, cmd("tenant_a", "rec_still_closed", "body"), scopeA, domain.KindLocalCheck, subject, nil)
		require.ErrorIs(t, err, domain.ErrStaleExecution, "an unresolved outcome keeps the scope restricted")
		restored, err := p1.dispatch.Get(ctx, h.Dispatch.ID, "", "")
		require.NoError(t, err)
		require.Equal(t, domain.DispatchUnknown, restored.State)
		require.Equal(t, int64(10_000), restored.Reserved.Amount, "unknown cost is never settled as zero")
	})
}

func countStatus(findings []*domain.RecoveryFinding, status domain.FindingStatus) int {
	n := 0
	for _, f := range findings {
		if f.Status == status {
			n++
		}
	}
	return n
}
