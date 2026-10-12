-- The price book, cost caps, pool administration and financial settlement
-- (P2.1, DD-02 §3). Lock ranks: cost caps, pools and allocations (1) before
-- the operation (2); caps are locked before allocations, both in id order.

-- name: InsertPriceRevision :exec
INSERT INTO price_revisions (
    revision, kind, route, provider, model, currency, effective_from, effective_until,
    input_units, output_units, reasoning_units, cached_input_units, max_exposure,
    input_includes_cached, output_includes_reasoning, digest, published_by, command_id, published_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19);

-- name: GetPriceRevision :one
SELECT * FROM price_revisions WHERE revision = $1;

-- name: LockPriceRevision :one
SELECT * FROM price_revisions WHERE revision = $1 FOR UPDATE;

-- name: WithdrawPriceRevision :exec
UPDATE price_revisions SET withdrawn_at = $2, withdrawn_by = $3 WHERE revision = $1 AND withdrawn_at IS NULL;

-- name: ListPriceRevisions :many
SELECT * FROM price_revisions ORDER BY published_at, revision;

-- name: CountPriceRevisions :one
SELECT count(*) FROM price_revisions;

-- name: GetCostCap :one
SELECT * FROM cost_caps WHERE level = $1 AND tenant_id = $2 AND project_id = $3 AND currency = $4;

-- name: LockCostCapByID :one
SELECT * FROM cost_caps WHERE cap_id = $1 FOR UPDATE;

-- name: InsertCostCap :exec
INSERT INTO cost_caps (cap_id, level, tenant_id, project_id, currency, cap_amount, reserved, consumed, revision, updated_by, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);

-- name: UpdateCostCapLimit :exec
UPDATE cost_caps SET cap_amount = $2, revision = $3, updated_by = $4, updated_at = $5 WHERE cap_id = $1;

-- name: UpdateCostCapCounters :exec
UPDATE cost_caps SET reserved = $2, consumed = $3, revision = $4 WHERE cap_id = $1;

-- name: LockOperationCostCaps :many
-- The caps that bind one operation: its tenant's, its project's and the
-- operation cap of its project (or of every project of the tenant).
SELECT c.* FROM cost_caps c
JOIN operations o ON o.operation_id = $1
WHERE c.tenant_id = o.tenant_id
  AND (c.level = 'tenant'
    OR (c.level = 'project' AND c.project_id = o.project_id)
    OR (c.level = 'operation' AND c.project_id IN (o.project_id, '')))
ORDER BY c.cap_id
FOR UPDATE OF c;

-- name: InsertPool :exec
INSERT INTO budget_pools (pool_id, level, tenant_id, actor_id, period_start, period_end, currency, cap_amount, allocated, revision, created_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);

-- name: GetPool :one
SELECT * FROM budget_pools WHERE pool_id = $1;

-- name: GetPoolByScope :one
SELECT * FROM budget_pools
WHERE level = sqlc.arg(level) AND coalesce(tenant_id, '') = sqlc.arg(tenant_key)::text
  AND coalesce(actor_id, '') = sqlc.arg(actor_key)::text AND period_start = sqlc.arg(period_start);

-- name: UpdatePoolCap :exec
UPDATE budget_pools SET cap_amount = $2, revision = $3 WHERE pool_id = $1;

-- name: InsertAllocationReturn :exec
INSERT INTO allocation_returns (allocation_id, pool_id, operation_id, currency, amount, returned_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: GetAllocationReturn :one
SELECT * FROM allocation_returns WHERE allocation_id = $1;

-- name: ListAllocationsByOperation :many
SELECT * FROM allocations WHERE operation_id = $1 ORDER BY pool_id;

-- name: ListFinanceUnsettled :many
SELECT operation_id FROM operations
WHERE finance_state IN ('not_funded', 'funded') AND lifecycle IN ('succeeded', 'failed', 'canceled')
ORDER BY updated_at
LIMIT $1;

-- name: GetOperationByID :one
SELECT * FROM operations WHERE operation_id = $1;
