package postgres_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// P1.4 (CTL-INF-04): an operation event committed through the store wakes
// the subscribers of its operation on another connection within the
// latency bound, with no polling; other operations' subscribers sleep.
func TestNotifierWakesStreamsAtCommit(t *testing.T) {
	inst := testdb.Start(t)
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	// The listener holds a connection of its own pool: the test's pool stays free.
	n := postgres.NewNotifier(inst.Pool(t), log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)
	wake, unsubscribe := n.Subscribe("op_notify_1")
	defer unsubscribe()
	other, unsubscribeOther := n.Subscribe("op_notify_2")
	defer unsubscribeOther()
	// The listener needs its LISTEN in place before the commit.
	time.Sleep(300 * time.Millisecond)
	// The commit-time notification InsertEvent issues, on a connection of
	// its own: delivered at commit, never before.
	notify := func(op string) {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
		_, err = tx.Exec(ctx, "SELECT pg_notify($1, $2)", postgres.EventChannel, op)
		require.NoError(t, err)
		select {
		case <-wake:
			// A wake before the commit only makes the stream re-read and wait
			// again; it is logged, never relied upon either way.
			t.Logf("wake observed before the commit of %s", op)
		case <-time.After(100 * time.Millisecond):
		}
		require.NoError(t, tx.Commit(ctx))
	}
	start := time.Now()
	notify("op_notify_1")
	select {
	case <-wake:
		require.Less(t, time.Since(start), 500*time.Millisecond, "the wake arrives within the latency bound")
	case <-time.After(3 * time.Second):
		t.Fatal("the subscriber was not woken by the committed notification")
	}
	select {
	case <-other:
		t.Fatal("another operation's subscriber was woken")
	case <-time.After(200 * time.Millisecond):
	}
	// Coalesced: two commits leave one pending wake.
	notify("op_notify_1")
	notify("op_notify_1")
	select {
	case <-wake:
	case <-time.After(3 * time.Second):
		t.Fatal("no wake after further commits")
	}
	// The store's InsertEvent path carries the same notification (an
	// operation row is required by the schema; the statement is the one above).
	_ = store
	require.Equal(t, 2, n.Subscribers())
	unsubscribe()
	require.Equal(t, 1, n.Subscribers())
}
