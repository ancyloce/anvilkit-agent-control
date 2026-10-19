package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

func lifecycleLatency() *postgres.LifecycleLatency {
	labels := []string{"kind", "profile", "lifecycle"}
	return &postgres.LifecycleLatency{
		FirstProgress: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "first_progress_seconds"}, labels),
		Completion:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "completion_seconds"}, labels),
	}
}

// observations sums a histogram's samples over all its series; for one
// label set it also returns the sum of that series.
func observations(t *testing.T, vec *prometheus.HistogramVec) (count uint64, series map[string]float64) {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	vec.Collect(ch)
	close(ch)
	series = map[string]float64{}
	for m := range ch {
		var out dto.Metric
		require.NoError(t, m.Write(&out))
		count += out.GetHistogram().GetSampleCount()
		key := ""
		for _, l := range out.GetLabel() {
			key += l.GetName() + "=" + l.GetValue() + ","
		}
		series[key] = out.GetHistogram().GetSampleSum()
	}
	return count, series
}

// P3.6 (platform.md SLO row 2): the store observes an operation's first
// progress and its completion after the commit of the transition that
// reaches them, once: never for a rolled-back transaction, never again for
// the same transition replayed from a stale read (its event is refused),
// never for later transitions, and never for an operation re-created from
// the inventory, whose first event is not its acceptance.
func TestLifecycleLatencyIsObservedOnceAtCommit(t *testing.T) {
	inst := testdb.Start(t)
	l := lifecycleLatency()
	store := postgres.NewStore(inst.Pool(t)).WithLifecycleLatency(l)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)
	profile := domain.Profile{ID: "local-check-v1", Kind: domain.KindLocalCheck, OperationDeadline: time.Hour, StepID: "local-check", JobProfileID: "local-check-v1"}
	subject := domain.Subject{ProfileID: "local-check-v1", SubjectDigest: application.DigestOf([]byte("subject"))}
	cmd := domain.CommandIdentity{TenantID: "tenant_a", CommandID: "cmd_latency", ActorID: "user_a", RequestDigest: application.DigestOf([]byte("body"))}

	op, err := domain.NewOperation(cmd, domain.Scope{TenantID: "tenant_a", ProjectID: "proj", ActorID: "user_a"}, domain.KindLocalCheck, subject, profile, t0)
	require.NoError(t, err)
	op.Revision = 0
	accepted := op.Transition("accepted", t0, func(*domain.Operation) {})
	require.NoError(t, store.Tx(ctx, func(r application.Repo) error {
		if err := r.InsertOperation(ctx, op); err != nil {
			return err
		}
		return r.InsertEvent(ctx, accepted)
	}))
	stale := *op // the record as a replica read it before the first progress

	transition := func(id string, at time.Time, apply func(*domain.Operation), fail error) error {
		return store.Tx(ctx, func(r application.Repo) error {
			locked, err := r.LockOperation(ctx, op.ID)
			if err != nil {
				return err
			}
			ev := locked.Transition(id, at, apply)
			if err := r.UpdateOperation(ctx, locked); err != nil {
				return err
			}
			if err := r.InsertEvent(ctx, ev); err != nil {
				return err
			}
			return fail
		})
	}
	running := func(o *domain.Operation) { o.Lifecycle, o.Phase = domain.LifecycleRunning, "running" }

	n, _ := observations(t, l.FirstProgress)
	require.Zero(t, n, "acceptance is no progress")
	require.Error(t, transition("attempt:a1:opened", t0.Add(2*time.Second), running, errors.New("rolled back after the event")))
	n, _ = observations(t, l.FirstProgress)
	require.Zero(t, n, "a rolled-back transition observes nothing")

	require.NoError(t, transition("attempt:a1:opened", t0.Add(2500*time.Millisecond), running, nil))
	n, series := observations(t, l.FirstProgress)
	require.Equal(t, uint64(1), n)
	require.Equal(t, map[string]float64{"kind=local_check,lifecycle=running,profile=local-check-v1,": 2.5}, series, "acceptance to the committed transition, by kind, profile and lifecycle")

	// The same transition replayed from the stale read (a retry after a
	// lost answer that re-applies instead of reading the recorded outcome):
	// its event is refused, nothing commits, nothing is observed.
	err = store.Tx(ctx, func(r application.Repo) error {
		replay := stale
		ev := replay.Transition("attempt:a1:opened", t0.Add(3*time.Second), running)
		if err := r.UpdateOperation(ctx, &replay); err != nil {
			return err
		}
		return r.InsertEvent(ctx, ev)
	})
	require.Error(t, err)
	n, _ = observations(t, l.FirstProgress)
	require.Equal(t, uint64(1), n, "a replayed transition observes nothing")

	require.NoError(t, transition("attempt:a1:closed", t0.Add(4*time.Second), func(o *domain.Operation) { o.Phase = "closed" }, nil))
	n, _ = observations(t, l.Completion)
	require.Zero(t, n)
	require.NoError(t, transition("attempt:a1:settled", t0.Add(5*time.Second), func(o *domain.Operation) {
		o.Lifecycle, o.Phase = domain.LifecycleSucceeded, "succeeded"
	}, nil))
	require.NoError(t, transition("finance:settled", t0.Add(6*time.Second), func(o *domain.Operation) { o.Finance = domain.FinanceSettled }, nil))
	n, _ = observations(t, l.FirstProgress)
	require.Equal(t, uint64(1), n, "later transitions are no first progress")
	n, series = observations(t, l.Completion)
	require.Equal(t, uint64(1), n, "the terminal commit only")
	require.Equal(t, map[string]float64{"kind=local_check,lifecycle=succeeded,profile=local-check-v1,": 5}, series)

	// An operation recovery re-created from the inventory starts
	// reconciling: neither milestone is its own.
	recovered := &domain.Operation{
		ID: "op_recovered_latency", TenantID: "tenant_a", ProjectID: "proj", ActorID: "recovery", CommandID: "recovery:op_recovered_latency", Kind: domain.KindLocalCheck,
		Subject: subject, SemanticDigest: application.DigestOf([]byte("recovered")), Lifecycle: domain.LifecycleReconciling, Phase: "recovered",
		Control: domain.ControlNone, Cleanup: domain.CleanupUnknown, Finance: domain.FinanceExposureUnknown, NextEventSeq: 1, ExecutionEpoch: 1, RecoveryEpoch: 2,
		Deadline: t0.Add(time.Hour), Intake: domain.IntakeConfirmed, Relay: domain.RelaySettled, CreatedAt: t0, UpdatedAt: t0,
	}
	restored := recovered.Transition("recovery:run:restored", t0, func(*domain.Operation) {})
	require.NoError(t, store.Tx(ctx, func(r application.Repo) error {
		if err := r.InsertOperation(ctx, recovered); err != nil {
			return err
		}
		return r.InsertEvent(ctx, restored)
	}))
	require.NoError(t, store.Tx(ctx, func(r application.Repo) error {
		locked, err := r.LockOperation(ctx, recovered.ID)
		if err != nil {
			return err
		}
		ev := locked.Transition("recovery:run:disposed", t0.Add(time.Minute), func(o *domain.Operation) {
			o.Lifecycle, o.Cleanup, o.Finance = domain.LifecycleFailed, domain.CleanupComplete, domain.FinanceFunded
		})
		if err := r.UpdateOperation(ctx, locked); err != nil {
			return err
		}
		return r.InsertEvent(ctx, ev)
	}))
	n, _ = observations(t, l.Completion)
	require.Equal(t, uint64(1), n, "a recovered operation's settlement is not observed")
	n, _ = observations(t, l.FirstProgress)
	require.Equal(t, uint64(1), n)
}
