package outbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmsql "github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/outbox"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// RB-A31: commit order is not offset order. Transaction A takes its outbox
// offset first and commits last; B takes the next one and commits first; R
// rolls back. The forwarder's subscriber must not deliver B while A, which
// precedes it, is still in progress (it would skip A forever once its offset
// moved past it), must then deliver A before B, never R, and a restarted
// subscriber resumes from its offsets with exactly what was not acknowledged.
// Every envelope is written through the real path: Repo.InsertEvent inside
// Store.Tx, as Control's use cases write them.
func TestOutboxReversedCommitAndRollback(t *testing.T) {
	inst := testdb.Start(t)
	pool := inst.Pool(t)
	store := postgres.NewStore(pool)
	ctx := context.Background()
	opA := newOperation(t, store, "tenant_a", "cmd_rev_a")
	opB := newOperation(t, store, "tenant_a", "cmd_rev_b")
	opR := newOperation(t, store, "tenant_a", "cmd_rev_r")

	inserted, release := make(chan struct{}), make(chan struct{})
	doneA := make(chan error, 1)
	go func() {
		doneA <- store.Tx(ctx, func(r application.Repo) error {
			if err := r.InsertEvent(ctx, event(opA, 1, domain.LifecycleAccepted)); err != nil {
				return err
			}
			close(inserted)
			<-release
			return nil
		})
	}()
	select {
	case <-inserted:
	case err := <-doneA:
		t.Fatalf("transaction A ended before its insert was held open: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("transaction A did not insert its event")
	}
	require.NoError(t, store.Tx(ctx, func(r application.Repo) error { return r.InsertEvent(ctx, event(opB, 1, domain.LifecycleAccepted)) }),
		"B commits while A is still open")
	abort := errors.New("abort after the event")
	require.ErrorIs(t, store.Tx(ctx, func(r application.Repo) error {
		if err := r.InsertEvent(ctx, event(opR, 1, domain.LifecycleAccepted)); err != nil {
			return err
		}
		return abort
	}), abort)

	newSubscriber := func() *wmsql.Subscriber {
		ack := 5 * time.Second
		sub, err := wmsql.NewSubscriber(wmsql.BeginnerFromPgx(pool), wmsql.SubscriberConfig{ConsumerGroup: "test-forwarder", AckDeadline: &ack,
			PollInterval: 50 * time.Millisecond, ResendInterval: 50 * time.Millisecond, RetryInterval: 50 * time.Millisecond,
			SchemaAdapter: outbox.Schema(10), OffsetsAdapter: outbox.Offsets()}, watermill.NopLogger{})
		require.NoError(t, err)
		return sub
	}
	receive := func(ch <-chan *message.Message, wait time.Duration) (*message.Message, bool) {
		select {
		case m := <-ch:
			return m, true
		case <-time.After(wait):
			return nil, false
		}
	}
	aggregate := func(m *message.Message) string {
		env, _, err := outbox.Unwrap(m.Payload)
		require.NoError(t, err)
		return env.AggregateID
	}

	sub := newSubscriber()
	subCtx, cancelSub := context.WithCancel(ctx)
	ch, err := sub.Subscribe(subCtx, outbox.ForwarderTopic)
	require.NoError(t, err)
	_, got := receive(ch, 700*time.Millisecond)
	require.False(t, got, "B stays invisible while the earlier transaction A is in progress")

	close(release)
	require.NoError(t, <-doneA)
	first, got := receive(ch, 5*time.Second)
	require.True(t, got)
	require.Equal(t, opA.ID, aggregate(first), "A, the earlier offset, comes first")
	first.Ack()
	second, got := receive(ch, 5*time.Second)
	require.True(t, got)
	require.Equal(t, opB.ID, aggregate(second))
	// B is not acknowledged before the subscriber closes: a restarted
	// subscriber resumes from the committed offsets with B, and nothing else.
	require.NoError(t, sub.Close())
	cancelSub()

	sub2 := newSubscriber()
	subCtx2, cancelSub2 := context.WithCancel(ctx)
	defer cancelSub2()
	ch2, err := sub2.Subscribe(subCtx2, outbox.ForwarderTopic)
	require.NoError(t, err)
	again, got := receive(ch2, 5*time.Second)
	require.True(t, got)
	require.Equal(t, opB.ID, aggregate(again), "the unacknowledged B is delivered again after a restart")
	again.Ack()
	extra, got := receive(ch2, 700*time.Millisecond)
	if got {
		t.Fatalf("nothing else is delivered (the rolled-back R never is), got %s", aggregate(extra))
	}
	require.NoError(t, sub2.Close())

	var stored int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&stored))
	require.Equal(t, 2, stored, "the rolled-back transaction left no envelope")
}
