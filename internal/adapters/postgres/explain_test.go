package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-control/internal/adapters/postgres/sqlc"
	"github.com/ancyloce/anvilkit-agent-control/internal/testdb"
)

// P1.5 (CTL-INF-06): every hot-path query of admission has an index that
// serves it. With sequential scans disabled the planner takes an index
// only when one matches the predicate; a query without one still shows a
// sequential scan, so each plan is required to name an index.
func TestHotPathQueriesHaveIndexes(t *testing.T) {
	inst := testdb.Start(t)
	pool := inst.Pool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, "SET enable_seqscan = off")
	require.NoError(t, err)
	queries := map[string]string{
		"LockAllocations":             "SELECT * FROM allocations WHERE operation_id = 'op' ORDER BY pool_id FOR UPDATE",
		"HasOverspend":                "SELECT EXISTS (SELECT 1 FROM dispatches d WHERE d.operation_id = 'op' AND d.state IN ('authorized', 'observed', 'unknown') AND (SELECT coalesce(sum(c.amount), 0) FROM cost_entries c WHERE c.dispatch_id = d.dispatch_id AND c.kind IN ('actual', 'correction')) > d.reserved_amount)",
		"ChargedCost":                 "SELECT coalesce(sum(amount), 0) FROM cost_entries WHERE dispatch_id = 'dsp' AND kind IN ('actual', 'correction')",
		"GetPermitByOwner":            "SELECT * FROM permits WHERE owner_kind = 'operation' AND owner_id = 'op' ORDER BY granted_at DESC LIMIT 1",
		"CountGrantSenders":           "SELECT count(*) FILTER (WHERE state = 'authorized'), count(*) FILTER (WHERE state = 'unknown') FROM dispatches WHERE grant_id = 'g' AND grant_revision = 1 AND state IN ('authorized', 'unknown')",
		"ListStagesByOperation":       "SELECT * FROM stage_manifests WHERE operation_id = 'op' ORDER BY accepted_at",
		"PreparedDispatchesByAttempt": "SELECT * FROM dispatches WHERE attempt_id = 'at' AND state = 'prepared' ORDER BY admitted_at",
		"HasUnknownDispatch":          "SELECT EXISTS (SELECT 1 FROM dispatches WHERE operation_id = 'op' AND state = 'unknown')",
		// P3.6: the obligation gauges' two branches stay inside the partial unresolved indexes.
		"UnresolvedDispatchGauges": "SELECT state, count(*), min(admitted_at) FROM dispatches WHERE state IN ('authorized', 'unknown') GROUP BY state",
		"UnresolvedEffectGauges":   "SELECT state, count(*), min(created_at) FROM effect_intents WHERE state IN ('permitted', 'unknown') GROUP BY state",
	}
	for name, q := range queries {
		rows, err := pool.Query(ctx, "EXPLAIN "+q)
		require.NoError(t, err, name)
		var plan []string
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			plan = append(plan, line)
		}
		rows.Close()
		text := strings.Join(plan, "\n")
		t.Logf("%s:\n%s", name, text)
		require.Contains(t, text, "Index", "%s has no index serving it:\n%s", name, text)
		require.NotContains(t, text, "Seq Scan on", "%s falls back to a sequential scan:\n%s", name, text)
	}
}

// P3.6: the obligation gauges' query runs against the migrated schema and
// reports nothing on an empty database.
func TestUnresolvedObligationsQuery(t *testing.T) {
	inst := testdb.Start(t)
	rows, err := sqlc.New(inst.Pool(t)).UnresolvedObligations(context.Background())
	require.NoError(t, err)
	require.Empty(t, rows)
}
