package application_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// TestGrantPolicyBarrier covers Control's side of the MCP grant barrier
// (P18, DD-08 §2) on a real PostgreSQL: idempotent registration receipts,
// conflicting policies refused, a revocation that fences new admission at
// once and converges only when no sender holds a consumed permission or an
// unknown outcome, a tombstone that keeps a late registration from making a
// revoked revision executable, and the share lock that makes the fence wait
// for an admission already deciding.
func TestGrantPolicyBarrier(t *testing.T) {
	inst := testdb.Start(t)
	fsInv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	inv := &hookedInventory{inner: fsInv, probe: inst.Pool(t)}
	auth := &authorityDouble{allowed: map[string]bool{"tenant_a/" + modelRoute: true}}
	prices := newPriceSource(fixturePrices(t, "fx-model-1", 1))
	p := newDispatchProcess(t, inst, inv, auth, prices)
	grants := application.NewGrantPolicies(postgres.NewStore(inst.Pool(t)), domain.SystemClock{}, testLog)
	ctx := context.Background()
	pools(t, p.pool, "grants", [3]int64{1_000_000_000, 100_000_000, 10_000_000})
	policy := func(b string) domain.Digest { return application.DigestOf([]byte(b)) }
	reg := func(grant string, digest domain.Digest) application.Registration {
		return application.Registration{GrantID: grant, Revision: 1, PolicyDigest: digest, ServerID: "srv", Methods: []string{"read"}, CostCap: &domain.Money{Currency: "USD", Amount: 100_000}}
	}
	tool := func(op *domain.Operation, at *domain.Attempt, grant, call string) domain.AdmissionRequest {
		return domain.AdmissionRequest{Kind: domain.DispatchTool, CallID: call, Owner: "mcp-a", OperationID: op.ID, AttemptID: at.ID, ExecutionEpoch: op.ExecutionEpoch,
			RouteID: toolRoute, GrantID: grant, GrantRevision: 1, ServerID: "srv", Method: "read", MaxExposure: usd(500), Deadline: time.Now().Add(time.Minute)}
	}

	t.Run("registration answers one receipt per revision and refuses conflicting policies", func(t *testing.T) {
		first, existing, err := grants.Register(ctx, cmd("tenant_a", "reg-a", "a"), reg("grant-a", policy("a")))
		require.NoError(t, err)
		require.False(t, existing)
		require.NotEmpty(t, first.ReceiptID)
		require.NotZero(t, first.PolicyEpoch)
		again, existing, err := grants.Register(ctx, cmd("tenant_a", "reg-a", "a"), reg("grant-a", policy("a")))
		require.NoError(t, err)
		require.True(t, existing, "a retried registration (lost answer, crash) answers the same receipt")
		require.Equal(t, first.ReceiptID, again.ReceiptID)
		other, existing, err := grants.Register(ctx, cmd("tenant_a", "reg-a2", "a2"), reg("grant-a", policy("a")))
		require.NoError(t, err)
		require.True(t, existing)
		require.Equal(t, first.ReceiptID, other.ReceiptID)
		_, _, err = grants.Register(ctx, cmd("tenant_a", "reg-a3", "a3"), reg("grant-a", policy("changed")))
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "a changed policy is never a replacement of a registered revision")
		_, _, err = grants.Register(ctx, cmd("tenant_a", "reg-a", "a"), reg("grant-other", policy("a")))
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "a command reused for another registration")
		_, _, err = grants.Register(ctx, cmd("tenant_b", "reg-b", "b"), reg("grant-a", policy("a")))
		require.ErrorIs(t, err, domain.ErrNotFound, "another tenant's grant")
		past := time.Now().Add(-time.Minute)
		expired := reg("grant-expired", policy("e"))
		expired.ExpiresAt = &past
		_, _, err = grants.Register(ctx, cmd("tenant_a", "reg-e", "e"), expired)
		require.ErrorIs(t, err, domain.ErrInvalid)
	})

	t.Run("a revision revoked before registration is a tombstone no late registration passes", func(t *testing.T) {
		p0, existing, err := grants.BeginRevocation(ctx, cmd("tenant_a", "rev-t", "t"), "grant-t", 1)
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, "converged", p0.RevocationState)
		_, _, err = grants.Register(ctx, cmd("tenant_a", "reg-t", "t"), reg("grant-t", policy("t")))
		require.ErrorIs(t, err, domain.ErrForbidden)
		op, at := funded(t, p, "tomb", 100_000)
		a, err := p.dispatch.Admit(ctx, admitCmd("call-tomb", "body"), tool(op, at, "grant-t", "call-tomb"))
		require.NoError(t, err)
		require.False(t, a.Allowed)
		require.Equal(t, domain.DenyForbidden, a.DenialCode)
	})

	t.Run("a tool dispatch binds its argument digest and effect class; only a free route admits zero exposure", func(t *testing.T) {
		r := reg("grant-w", policy("w"))
		r.Methods = []string{"read", "note"}
		_, _, err := grants.Register(ctx, cmd("tenant_a", "reg-w", "w"), r)
		require.NoError(t, err)
		op, at := funded(t, p, "free-write", 100_000)
		args := application.DigestOf([]byte(`{"title":"x"}`))
		write := tool(op, at, "grant-w", "call-w1")
		write.RouteID, write.Method, write.MaxExposure = freeToolRoute, "note", usd(0)
		write.ArgumentDigest, write.SideEffecting = args, true
		a, err := p.dispatch.Admit(ctx, admitCmd("call-w1", "body"), write)
		require.NoError(t, err)
		require.True(t, a.Allowed, "a free write is admitted (with its obligation) at zero exposure")
		body, _, err := fsInv.Get(ctx, "tool-dispatch/"+a.Dispatch.ID)
		require.NoError(t, err)
		require.Contains(t, string(body), `"argumentDigest":"`+string(args)+`"`)
		require.Contains(t, string(body), `"sideEffecting":true`)
		other := write
		other.ArgumentDigest = application.DigestOf([]byte(`{"title":"y"}`))
		_, err = p.dispatch.Admit(ctx, admitCmd("call-w1", "body"), other)
		require.ErrorIs(t, err, domain.ErrIdempotencyConflict, "the same call never admits other arguments")
		read := tool(op, at, "grant-w", "call-w2")
		read.MaxExposure = usd(0)
		zero, err := p.dispatch.Admit(ctx, admitCmd("call-w2", "body"), read)
		require.NoError(t, err)
		require.False(t, zero.Allowed, "a paid route never admits zero exposure")
		require.Equal(t, domain.DenyInvalidArgument, zero.DenialCode)
	})

	t.Run("the fence denies new admission at once; convergence waits for consumed and unknown permissions", func(t *testing.T) {
		_, _, err := grants.Register(ctx, cmd("tenant_a", "reg-f", "f"), reg("grant-f", policy("f")))
		require.NoError(t, err)
		op, at := funded(t, p, "fence", 100_000)
		first := tool(op, at, "grant-f", "call-f1")
		sent, err := p.dispatch.Admit(ctx, admitCmd("call-f1", "body"), first)
		require.NoError(t, err)
		require.True(t, sent.Allowed)
		lost, err := p.dispatch.Admit(ctx, admitCmd("call-f2", "body"), tool(op, at, "grant-f", "call-f2"))
		require.NoError(t, err)
		require.True(t, lost.Allowed)
		_, _, err = p.dispatch.Observe(ctx, application.Reader{}, lost.Dispatch.ID, "mcp-a", 1, domain.DispatchOutcomeUnknown, nil, "", time.Now())
		require.NoError(t, err)

		fenced, existing, err := grants.BeginRevocation(ctx, cmd("tenant_a", "rev-f", "rf"), "grant-f", 1)
		require.NoError(t, err)
		require.False(t, existing)
		require.Equal(t, "fenced", fenced.RevocationState)
		require.Equal(t, [2]uint64{1, 1}, [2]uint64{fenced.InFlightCalls, fenced.UnknownCalls})
		again, existing, err := grants.BeginRevocation(ctx, cmd("tenant_a", "rev-f-retry", "rf2"), "grant-f", 1)
		require.NoError(t, err)
		require.True(t, existing, "a repeated revocation answers the existing barrier")
		require.Equal(t, fenced.FencedAt.UTC(), again.FencedAt.UTC())
		_, _, err = grants.Register(ctx, cmd("tenant_a", "reg-f-late", "f"), reg("grant-f", policy("f")))
		require.ErrorIs(t, err, domain.ErrForbidden, "a revoked revision is never registered again")

		denied, err := p.dispatch.Admit(ctx, admitCmd("call-f3", "body"), tool(op, at, "grant-f", "call-f3"))
		require.NoError(t, err)
		require.False(t, denied.Allowed, "blocked new admission")
		require.Equal(t, domain.DenyForbidden, denied.DenialCode)
		replay, err := p.dispatch.Admit(ctx, admitCmd("call-f1", "body"), first)
		require.NoError(t, err)
		require.False(t, replay.Allowed, "an earlier permission is never issued twice")

		status, err := grants.Revocation(ctx, "grant-f", 1)
		require.NoError(t, err)
		require.Equal(t, "converging", status.RevocationState, "sent and unknown senders are not quiesced")
		_, _, err = p.dispatch.Observe(ctx, application.Reader{}, sent.Dispatch.ID, "mcp-a", 1, domain.DispatchSucceeded, reported(domain.Usage{}), "", time.Now())
		require.NoError(t, err)
		status, err = grants.Revocation(ctx, "grant-f", 1)
		require.NoError(t, err)
		require.Equal(t, "converging", status.RevocationState)
		require.Equal(t, [2]uint64{0, 1}, [2]uint64{status.InFlightCalls, status.UnknownCalls}, "the unknown outcome keeps the barrier open")
		_, err = p.pool.Exec(ctx, "UPDATE dispatches SET state = 'confirmed_not_sent' WHERE dispatch_id = $1", lost.Dispatch.ID)
		require.NoError(t, err)
		status, err = grants.Revocation(ctx, "grant-f", 1)
		require.NoError(t, err)
		require.Equal(t, "converged", status.RevocationState)
		require.NotNil(t, status.ConvergedAt)
		_, err = grants.Revocation(ctx, "grant-a", 1)
		require.ErrorIs(t, err, domain.ErrNotFound, "no revocation of an active grant")
	})

	t.Run("the fence waits for an admission already holding the policy's share lock", func(t *testing.T) {
		_, _, err := grants.Register(ctx, cmd("tenant_a", "reg-l", "l"), reg("grant-l", policy("l")))
		require.NoError(t, err)
		tx, err := p.pool.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "SELECT 1 FROM grant_policies WHERE grant_id = 'grant-l' AND grant_revision = 1 FOR SHARE")
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() {
			_, _, err := grants.BeginRevocation(ctx, cmd("tenant_a", "rev-l", "rl"), "grant-l", 1)
			done <- err
		}()
		select {
		case err := <-done:
			t.Fatalf("the fence committed while an admission held the share lock: %v", err)
		case <-time.After(500 * time.Millisecond):
		}
		require.NoError(t, tx.Commit(ctx))
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("the fence did not commit after the admission released its lock")
		}
	})
}
