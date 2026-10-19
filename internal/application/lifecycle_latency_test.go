package application_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/inventory"
	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres"
	"github.com/ancyloce/anvilkit-agent-control/internal/application"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// latencySeries reads every series of a lifecycle histogram: the sample
// count and sum by "kind/profile/lifecycle".
func latencySeries(t *testing.T, vec *prometheus.HistogramVec) map[string][2]float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	vec.Collect(ch)
	close(ch)
	out := map[string][2]float64{}
	for m := range ch {
		var d dto.Metric
		require.NoError(t, m.Write(&d))
		labels := map[string]string{}
		for _, l := range d.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		key := labels["kind"] + "/" + labels["profile"] + "/" + labels["lifecycle"]
		h := d.GetHistogram()
		if h.GetSampleCount() > 0 {
			out[key] = [2]float64{float64(h.GetSampleCount()), h.GetSampleSum()}
		}
	}
	return out
}

// merged adds the series of several replicas: what Prometheus sums.
func merged(t *testing.T, vecs ...*prometheus.HistogramVec) map[string][2]float64 {
	out := map[string][2]float64{}
	for _, v := range vecs {
		for k, s := range latencySeries(t, v) {
			out[k] = [2]float64{out[k][0] + s[0], out[k][1] + s[1]}
		}
	}
	return out
}

// P3.6 (platform.md SLO row 2) through the use cases on two replicas: the
// first progress of an operation is observed once, by the replica that
// committed it, when both replicas race the same command and when the
// command is retried after its answer was lost; a cancel of an accepted
// operation is its first progress and its completion, each once.
func TestLifecycleLatencyIsObservedOncePerOperationAcrossReplicas(t *testing.T) {
	inst := testdb.Start(t)
	inv, err := inventory.NewFilesystem(filepath.Join(t.TempDir(), "inventory"))
	require.NoError(t, err)
	clock := &manualClock{now: time.Now().UTC().Truncate(time.Microsecond)}
	type replica struct {
		ops     *application.Operations
		exec    *application.Execution
		latency *postgres.LifecycleLatency
	}
	newReplica := func() replica {
		labels := []string{"kind", "profile", "lifecycle"}
		l := &postgres.LifecycleLatency{
			FirstProgress: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "first_progress_seconds"}, labels),
			Completion:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "completion_seconds"}, labels),
		}
		store := postgres.NewStore(inst.Pool(t)).WithLifecycleLatency(l)
		return replica{
			ops:     application.NewOperations(store, inv, []domain.Profile{profile}, clock, testLog),
			exec:    application.NewExecution(store, inv, manifests, []domain.Profile{profile}, clock, testLog),
			latency: l,
		}
	}
	r1, r2 := newReplica(), newReplica()
	ctx := context.Background()
	firstProgress := func() map[string][2]float64 { return merged(t, r1.latency.FirstProgress, r2.latency.FirstProgress) }
	completion := func() map[string][2]float64 { return merged(t, r1.latency.Completion, r2.latency.Completion) }

	op, _, err := r1.ops.Create(ctx, cmd("tenant_a", "cmd_latency", "body"), scopeA, domain.KindLocalCheck, subject, nil)
	require.NoError(t, err)
	require.Empty(t, firstProgress(), "acceptance and intake confirmation are no progress")

	clock.Advance(1500 * time.Millisecond)
	open := cmd("tenant_a", "cmd_latency_open", "open")
	var wg sync.WaitGroup
	existing := make(chan bool, 2)
	for _, r := range []replica{r1, r2} {
		wg.Add(1)
		go func(r replica) {
			defer wg.Done()
			_, ex, err := r.exec.OpenAttempt(ctx, open, op.ID, "local-check", 0, "local-check-v1")
			require.NoError(t, err)
			existing <- ex
		}(r)
	}
	wg.Wait()
	close(existing)
	fresh := 0
	for ex := range existing {
		if !ex {
			fresh++
		}
	}
	require.Equal(t, 1, fresh, "one replica committed the attempt")
	require.Equal(t, map[string][2]float64{"local_check/local-check-v1/running": {1, 1.5}}, firstProgress(),
		"observed once, by the committing replica, from acceptance to the commit")

	// The answer was lost: the Workflow retries the same command on the
	// other replica, which answers the recorded attempt.
	clock.Advance(time.Second)
	_, ex, err := r2.exec.OpenAttempt(ctx, open, op.ID, "local-check", 0, "local-check-v1")
	require.NoError(t, err)
	require.True(t, ex)
	require.Equal(t, map[string][2]float64{"local_check/local-check-v1/running": {1, 1.5}}, firstProgress(), "a retried command observes nothing")
	require.Empty(t, completion())

	// A cancel of an accepted operation takes it out of acceptance and ends it.
	other, _, err := r1.ops.Create(ctx, cmd("tenant_a", "cmd_latency_cancel_op", "body"), scopeA, domain.KindLocalCheck, subject, nil)
	require.NoError(t, err)
	clock.Advance(4 * time.Second)
	cancel := cmd("tenant_a", "cmd_latency_cancel", "cancel")
	c, _, err := r2.ops.SubmitCommand(ctx, cancel, scopeA, other.ID, domain.CommandCancel, other.Revision, "")
	require.NoError(t, err)
	require.Equal(t, domain.OutcomeApplied, c.Outcome)
	_, ex, err = r1.ops.SubmitCommand(ctx, cancel, scopeA, other.ID, domain.CommandCancel, other.Revision, "")
	require.NoError(t, err)
	require.True(t, ex)
	require.Equal(t, map[string][2]float64{"local_check/local-check-v1/running": {1, 1.5}, "local_check/local-check-v1/canceled": {1, 4}}, firstProgress())
	require.Equal(t, map[string][2]float64{"local_check/local-check-v1/canceled": {1, 4}}, completion(), "the terminal commit, once")
}
