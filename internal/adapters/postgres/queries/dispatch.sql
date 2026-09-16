-- Budgets, single-use dispatch admission, metering and the grant-policy
-- projection (DD-02 §3–§4, delivery.md P06). Lock ranks: pools and
-- allocations (1) before the operation (2), attempt/instance (3), the
-- dispatch (4) and the ledger (6); the application acquires them in that
-- order and never performs I/O while any is held.

-- name: ListCurrentPools :many
SELECT * FROM budget_pools
WHERE currency = $1 AND period_start <= $2 AND period_end > $2
  AND ((level = 'platform' AND tenant_id IS NULL AND actor_id IS NULL)
    OR (level = 'tenant' AND tenant_id = $3 AND actor_id IS NULL)
    OR (level = 'actor' AND tenant_id = $3 AND actor_id = $4))
ORDER BY pool_id;

-- name: LockPool :one
SELECT * FROM budget_pools WHERE pool_id = $1 FOR UPDATE;

-- name: UpdatePool :exec
UPDATE budget_pools SET allocated = $2, revision = $3 WHERE pool_id = $1;

-- name: LockAllocations :many
SELECT * FROM allocations WHERE operation_id = $1 ORDER BY pool_id FOR UPDATE;

-- name: GetAllocationByPool :one
SELECT * FROM allocations WHERE pool_id = $1 AND operation_id = $2;

-- name: InsertAllocation :exec
INSERT INTO allocations (allocation_id, pool_id, operation_id, currency, amount, reserved, consumed, revision, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: UpdateAllocation :exec
UPDATE allocations SET reserved = $2, consumed = $3, revision = $4 WHERE allocation_id = $1;

-- name: GetDispatchByCall :one
SELECT * FROM dispatches WHERE tenant_id = $1 AND owner = $2 AND call_id = $3;

-- name: FindDispatchesByOwnerCall :many
SELECT * FROM dispatches WHERE owner = $1 AND call_id = $2 ORDER BY admitted_at LIMIT 2;

-- name: GetDispatch :one
SELECT * FROM dispatches WHERE dispatch_id = $1;

-- name: LockDispatch :one
SELECT * FROM dispatches WHERE dispatch_id = $1 FOR UPDATE;

-- name: InsertDispatch :exec
INSERT INTO dispatches (
    dispatch_id, kind, tenant_id, operation_id, attempt_id, instance_id, call_id, owner, request_digest, route_id,
    grant_id, grant_revision, execution_epoch, state, outcome, denial_code, reserved_currency, reserved_amount, meter_revision,
    supersedes_call_id, evidence_ref, inventory_state, inventory_version, deadline, admitted_at, observed_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19,
    $20, $21, $22, $23, $24, $25, $26
);

-- name: UpdateDispatch :exec
UPDATE dispatches SET state = $2, outcome = $3, denial_code = $4, evidence_ref = $5, inventory_state = $6, inventory_version = $7, observed_at = $8
WHERE dispatch_id = $1;

-- name: HasOverspend :one
SELECT EXISTS (
    SELECT 1 FROM dispatches d
    WHERE d.operation_id = $1 AND d.state IN ('authorized', 'observed', 'unknown')
      AND (SELECT coalesce(sum(c.amount), 0) FROM cost_entries c WHERE c.dispatch_id = d.dispatch_id AND c.kind IN ('actual', 'correction')) > d.reserved_amount
);

-- name: HasUnknownDispatch :one
SELECT EXISTS (SELECT 1 FROM dispatches WHERE operation_id = $1 AND state = 'unknown');

-- name: GetUsageObservation :one
SELECT * FROM usage_observations WHERE dispatch_id = $1 AND source = $2 AND sequence = $3;

-- name: InsertUsageObservation :exec
INSERT INTO usage_observations (observation_id, dispatch_id, source, sequence, native_reference, input_units, output_units, reasoning_units, cached_input_units, cost_revision, observed_at, usage_reported)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);

-- MaxUsage is the running maximum of the explicitly reported counters; a
-- report without usage (usage_reported = false) never enters it.
-- name: MaxUsage :one
SELECT coalesce(max(input_units), 0)::bigint AS input_units, coalesce(max(output_units), 0)::bigint AS output_units,
       coalesce(max(reasoning_units), 0)::bigint AS reasoning_units, coalesce(max(cached_input_units), 0)::bigint AS cached_input_units
FROM usage_observations WHERE dispatch_id = $1 AND usage_reported;

-- name: HasReportedUsage :one
SELECT EXISTS (SELECT 1 FROM usage_observations WHERE dispatch_id = $1 AND usage_reported);

-- name: ChargedCost :one
SELECT coalesce(sum(amount), 0)::bigint AS amount, coalesce(min(entry_id) FILTER (WHERE kind = 'actual'), '')::text AS actual_entry_id
FROM cost_entries WHERE dispatch_id = $1 AND kind IN ('actual', 'correction');

-- name: InsertCostEntry :exec
INSERT INTO cost_entries (entry_id, operation_id, dispatch_id, observation_id, kind, currency, amount, correction_of, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: GetGrantPolicy :one
SELECT * FROM grant_policies WHERE grant_id = $1 AND grant_revision = $2;
