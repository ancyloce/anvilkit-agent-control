package retention_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/retention"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

var profile = domain.Profile{ID: "local-check-v1", Kind: domain.KindLocalCheck, OperationDeadline: time.Hour, StepID: "local-check"}

// history commits an operation through the store as the application does:
// acceptance, optionally an attempt-bound event, then the transitions of
// final, all at the time at.
func history(t *testing.T, store *postgres.Store, id string, at time.Time, step bool, final ...func(*domain.Operation)) {
	t.Helper()
	ctx := context.Background()
	subject := domain.Subject{ProfileID: profile.ID, SubjectDigest: application.DigestOf([]byte("subject"))}
	cmd := domain.CommandIdentity{TenantID: "tenant_a", CommandID: "cmd_" + id, ActorID: "user_a", RequestDigest: application.DigestOf([]byte(id))}
	op, err := domain.NewOperation(cmd, domain.Scope{TenantID: "tenant_a", ProjectID: "proj", ActorID: "user_a"}, domain.KindLocalCheck, subject, profile, at)
	require.NoError(t, err)
	op.ID = id
	op.Revision = 0
	accepted := op.Transition("accepted", at, func(*domain.Operation) {})
	require.NoError(t, store.Tx(ctx, func(r application.Repo) error {
		if err := r.InsertOperation(ctx, op); err != nil {
			return err
		}
		return r.InsertEvent(ctx, accepted)
	}))
	transition := func(tid string, apply func(*domain.Operation), bind bool) {
		require.NoError(t, store.Tx(ctx, func(r application.Repo) error {
			locked, err := r.LockOperation(ctx, id)
			if err != nil {
				return err
			}
			ev := locked.Transition(tid, at, apply)
			if bind {
				ev.Payload.Step, ev.Payload.Visit, ev.Payload.AttemptOrdinal = "local-check", "0", "1"
			}
			if err := r.UpdateOperation(ctx, locked); err != nil {
				return err
			}
			return r.InsertEvent(ctx, ev)
		}))
	}
	if step {
		transition("attempt:opened", func(o *domain.Operation) { o.Lifecycle, o.Phase = domain.LifecycleRunning, "running" }, true)
	}
	for i, apply := range final {
		transition("final:"+string(rune('a'+i)), apply, false)
	}
}

func succeeded(o *domain.Operation) { o.Lifecycle, o.Phase = domain.LifecycleSucceeded, "closed" }
func settled(o *domain.Operation) {
	o.Finance, o.Cleanup = domain.FinanceSettled, domain.CleanupComplete
}

func seqs(t *testing.T, pool *pgxpool.Pool, id string) []int64 {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT event_seq FROM operation_events WHERE operation_id = $1 ORDER BY event_seq", id)
	require.NoError(t, err)
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var n int64
		require.NoError(t, rows.Scan(&n))
		out = append(out, n)
	}
	require.NoError(t, rows.Err())
	return out
}

// TestOperationEventRetention (RB-A27): under its own identity the
// retention deletes the event prefix of settled terminal operations
// unchanged past the period, keeps the newest attempt-bound event and every
// later one, touches nothing that is recent, unsettled, uncleaned or still
// running, and a cursor before the kept suffix is reset. The identity is
// verified: never the app role, never the table owner.
func TestOperationEventRetention(t *testing.T) {
	inst := testdb.Start(t)
	app := inst.Pool(t)
	store := postgres.NewStore(app)
	ctx := context.Background()
	old, now := time.Now().Add(-100*24*time.Hour), time.Now()

	history(t, store, "op_old_step", old, true, succeeded, settled)   // events 1..4, step at 2
	history(t, store, "op_old_plain", old, false, succeeded, settled) // events 1..3, no step
	history(t, store, "op_old_extra", old, false, succeeded, settled) // a second candidate for the batch bound
	history(t, store, "op_recent", now, true, succeeded, settled)
	history(t, store, "op_unknown", old, false, succeeded, func(o *domain.Operation) {
		o.Finance, o.Cleanup = domain.FinanceExposureUnknown, domain.CleanupComplete
	})
	history(t, store, "op_cleanup", old, false, succeeded, func(o *domain.Operation) { o.Finance, o.Cleanup = domain.FinanceSettled, domain.CleanupPending })
	history(t, store, "op_running", old, true)

	pool, err := pgxpool.New(ctx, inst.RetentionDSN)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, retention.VerifyIdentity(ctx, pool, "anvilkit_control_app"))
	require.ErrorIs(t, retention.VerifyIdentity(ctx, app, "anvilkit_control_app"), retention.ErrIdentity, "the app role is refused")
	require.ErrorIs(t, retention.VerifyIdentity(ctx, app, "someone_else"), retention.ErrIdentity, "the app role cannot delete events")
	// The table owner (the migrator) on the Control database.
	owner, err := pgxpool.New(ctx, strings.Replace(inst.RetentionDSN, "anvilkit_control_retention:retention@", "anvilkit_control_migrator:migrator@", 1))
	require.NoError(t, err)
	t.Cleanup(owner.Close)
	require.ErrorIs(t, retention.VerifyIdentity(ctx, owner, "anvilkit_control_app"), retention.ErrIdentity, "a role that may append events is refused")

	// The identity deletes events and nothing else.
	for _, stmt := range []string{
		"UPDATE operations SET phase = phase",
		"INSERT INTO operation_events SELECT * FROM operation_events LIMIT 0",
		"DELETE FROM operations WHERE false",
		"DELETE FROM attempts WHERE false",
	} {
		_, err := pool.Exec(ctx, stmt)
		require.ErrorContains(t, err, "permission denied", stmt)
	}

	before := now.Add(-30 * 24 * time.Hour)
	events, ops, err := retention.PruneOperationEvents(ctx, pool, before, 1)
	require.NoError(t, err)
	require.Equal(t, 1, ops, "a pass prunes at most batch operations")
	require.Positive(t, events)
	events2, ops2, err := retention.PruneOperationEvents(ctx, pool, before, 100)
	require.NoError(t, err)
	require.Equal(t, 2, ops2)
	require.Equal(t, int64(1+3+3), events+events2, "the prefix before the step event, and both plain histories")
	events, ops, err = retention.PruneOperationEvents(ctx, pool, before, 100)
	require.NoError(t, err)
	require.Zero(t, ops, "a pruned history is no candidate again")
	require.Zero(t, events)

	require.Equal(t, []int64{2, 3, 4}, seqs(t, app, "op_old_step"), "the snapshot's progress event and every later one stay")
	require.Empty(t, seqs(t, app, "op_old_plain"))
	require.Empty(t, seqs(t, app, "op_old_extra"))
	require.Equal(t, []int64{1, 2, 3, 4}, seqs(t, app, "op_recent"), "within the period")
	require.Equal(t, []int64{1, 2, 3}, seqs(t, app, "op_unknown"), "an unknown exposure keeps its history")
	require.Equal(t, []int64{1, 2, 3}, seqs(t, app, "op_cleanup"), "an unresolved cleanup keeps its history")
	require.Equal(t, []int64{1, 2}, seqs(t, app, "op_running"), "a running operation keeps its history")

	ops3 := application.NewOperations(store, nil, []domain.Profile{profile}, domain.SystemClock{}, slog.Default())
	scope := domain.Scope{TenantID: "tenant_a", ProjectID: "proj", ActorID: "user_a"}
	for _, tc := range []struct {
		id    string
		after uint64
		limit int
		reset bool
	}{
		{"op_old_step", 0, 200, true},
		{"op_old_step", 0, 1, true}, // the API's one-event preflight sees it too
		{"op_old_step", 1, 1, false},
		{"op_old_step", 4, 200, false},
		{"op_old_plain", 0, 200, true},
		{"op_old_plain", 3, 200, false},
		{"op_recent", 0, 200, false},
	} {
		page, err := ops3.ListEvents(ctx, scope, tc.id, tc.after, tc.limit)
		require.NoError(t, err)
		require.Equal(t, tc.reset, page.ResetRequired, "%s after %d limit %d", tc.id, tc.after, tc.limit)
	}
	progress, err := ops3.Get(ctx, scope, "op_old_step")
	require.NoError(t, err)
	latest, err := ops3.LatestProgress(ctx, progress)
	require.NoError(t, err)
	require.NotNil(t, latest, "the snapshot keeps its progress")
	require.Equal(t, uint64(2), latest.EventSeq)
}
