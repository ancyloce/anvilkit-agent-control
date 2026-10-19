package authority_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/authority"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// TestDecisionAgeIsObservedAtEachUse (P3.6, platform.md SLO row 3): every
// decision Control relies on is observed with its age at that use — 0 when
// fetched, the time since the fetch for a cache hit — by kind and
// decision; a failed fetch relies on nothing and observes nothing.
func TestDecisionAgeIsObservedAtEachUse(t *testing.T) {
	d := newDecisionSource(t)
	c := &clock{now: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)}
	d.set(allow(c.now.Add(time.Minute)))
	age := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "decision_age_seconds", Buckets: []float64{0.001, 10, 30}}, []string{"kind", "decision"})
	s, err := authority.New(authority.Options{URL: d.srv.URL + "/decide", Freshness: 30 * time.Second, Timeout: time.Second, DecisionAge: age}, c)
	require.NoError(t, err)
	ctx := context.Background()
	read := func(kind, decision string) (uint64, float64) {
		var m dto.Metric
		require.NoError(t, age.WithLabelValues(kind, decision).(prometheus.Metric).Write(&m))
		return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
	}

	_, err = s.CheckExecution(ctx, scope, "route")
	require.NoError(t, err)
	n, sum := read("execution", "allow")
	require.Equal(t, uint64(1), n)
	require.Zero(t, sum, "a decision just fetched is 0 s old")
	c.advance(12 * time.Second)
	_, err = s.CheckExecution(ctx, scope, "route")
	require.NoError(t, err)
	n, sum = read("execution", "allow")
	require.Equal(t, uint64(2), n)
	require.Equal(t, 12.0, sum, "a hit is as old as its fetch")
	require.EqualValues(t, 1, d.requests.Load())

	d.set(func(map[string]any) (int, string) {
		return 200, `{"decision":"deny","revision":"rev-9","freshUntil":"` + c.Now().Add(time.Minute).Format(time.RFC3339Nano) + `"}`
	})
	q := domain.AuthorizationQuery{Scope: scope, ResourceType: "knowledge-snapshot", ResourceID: "snap_1", Action: "read", Disclosure: true}
	_, err = s.Check(ctx, q)
	require.NoError(t, err)
	n, _ = read("disclosure", "deny")
	require.Equal(t, uint64(1), n)

	d.down.Store(true)
	c.advance(31 * time.Second)
	_, err = s.Check(ctx, q)
	require.ErrorIs(t, err, domain.ErrUnavailable)
	n, _ = read("disclosure", "deny")
	require.Equal(t, uint64(1), n, "an unavailable decision is not observed")
}
