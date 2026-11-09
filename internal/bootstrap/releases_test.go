package bootstrap

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/domain"
)

// RB-A26: the release gauges expose every in-flight state from the start, set
// each read's rows and reset a state that has no row; only states are labels.
func TestReleaseGaugesByState(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := newReleaseGauges(reg)
	values := func() map[string][2]float64 {
		families, err := reg.Gather()
		require.NoError(t, err)
		out := map[string][2]float64{}
		for _, f := range families {
			for _, m := range f.GetMetric() {
				if f.GetName() == "anvilkit_control_release_state_reads_failed_total" {
					continue
				}
				require.Len(t, m.GetLabel(), 1)
				require.Equal(t, "state", m.GetLabel()[0].GetName())
				v := out[m.GetLabel()[0].GetValue()]
				switch f.GetName() {
				case "anvilkit_control_releases_in_flight":
					v[0] = m.GetGauge().GetValue()
				case "anvilkit_control_release_oldest_seconds":
					v[1] = m.GetGauge().GetValue()
				}
				out[m.GetLabel()[0].GetValue()] = v
			}
		}
		return out
	}
	zero := map[string][2]float64{}
	for _, s := range []domain.ReleaseState{domain.ReleaseCertifying, domain.ReleaseAwaitingApproval, domain.ReleasePublishing, domain.ReleasePartiallyPublished, domain.ReleaseReconciling} {
		zero[string(s)] = [2]float64{}
	}
	require.Equal(t, zero, values(), "every in-flight state exists at 0 before the first read")

	g.set([]sqlc.ReleaseStatesRow{{State: "partially_published", Releases: 2, OldestSeconds: 1200}, {State: "publishing", Releases: 1, OldestSeconds: 30}})
	want := map[string][2]float64{}
	for k, v := range zero {
		want[k] = v
	}
	want["partially_published"] = [2]float64{2, 1200}
	want["publishing"] = [2]float64{1, 30}
	require.Equal(t, want, values())

	g.set(nil)
	require.Equal(t, zero, values(), "a state with no row returns to 0")
}
