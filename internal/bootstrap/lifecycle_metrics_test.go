package bootstrap

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
)

// RB-A25: the execution pool gauges carry each pool's active permits, the
// limit it admits (capacity less the control reserve) and the oldest
// permit's age; the lifecycle gauges name the one active generation and
// record the last stop's drain and whether it was forced.
func TestExecutionPoolAndLifecycleGauges(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := newExecutionPoolGauges(reg)
	g.set([]sqlc.ExecutionPoolUsageRow{{PoolID: "generation", Capacity: 3, ReservedControl: 1, Active: 2, OldestSeconds: 95}})
	require.Equal(t, 2.0, gauge(t, g.active.WithLabelValues("generation")))
	require.Equal(t, 2.0, gauge(t, g.limit.WithLabelValues("generation")))
	require.Equal(t, 95.0, gauge(t, g.oldest.WithLabelValues("generation")))
	g.set([]sqlc.ExecutionPoolUsageRow{{PoolID: "generation", Capacity: 0, ReservedControl: 1}})
	require.Equal(t, 0.0, gauge(t, g.limit.WithLabelValues("generation")), "a reserve above the capacity admits nothing, never a negative limit")
	require.Equal(t, 0.0, gauge(t, g.active.WithLabelValues("generation")))

	lm := newLifecycleMetrics(reg)
	families, err := reg.Gather()
	require.NoError(t, err)
	byName := map[string]float64{}
	for _, f := range families {
		if len(f.GetMetric()) == 1 && f.GetMetric()[0].GetGauge() != nil {
			byName[f.GetName()] = f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	require.Equal(t, 1.0, byName["anvilkit_control_config_generation"])
	require.Zero(t, byName["anvilkit_control_shutdown_forced"])
	lm.stopped(1500*time.Millisecond, false)
	require.Equal(t, 1.5, gauge(t, lm.drain))
	require.Zero(t, gauge(t, lm.forced), "a drained stop is not forced")
	lm.stopped(20*time.Second, true)
	require.Equal(t, 1.0, gauge(t, lm.forced))
}

// gauge reads one gauge's value.
func gauge(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, g.Write(&m))
	return m.GetGauge().GetValue()
}
