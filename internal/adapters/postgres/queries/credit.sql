-- Credit pools and credit entries (DEC-066, migration 00020): whole-credit
-- limits per tenant or actor and period, debited once per operation at
-- funding beside its USD allocations and never refunded. Lock rank 1, after
-- the budget pools, in id order.

-- name: ListCurrentCreditPools :many
SELECT * FROM credit_pools
WHERE tenant_id = sqlc.arg(tenant_id)::text
  AND period_start <= sqlc.arg(at) AND period_end > sqlc.arg(at)
  AND ((level = 'tenant' AND actor_id IS NULL) OR (level = 'actor' AND actor_id = sqlc.arg(actor_id)::text))
ORDER BY credit_pool_id;

-- name: LockCreditPool :one
SELECT * FROM credit_pools WHERE credit_pool_id = $1 FOR UPDATE;

-- name: GetCreditPoolByScope :one
SELECT * FROM credit_pools
WHERE level = sqlc.arg(level) AND tenant_id = sqlc.arg(tenant_id)::text
  AND coalesce(actor_id, '') = sqlc.arg(actor_key)::text AND period_start = sqlc.arg(period_start);

-- name: InsertCreditPool :exec
INSERT INTO credit_pools (credit_pool_id, level, tenant_id, actor_id, period_start, period_end, limit_credits, used_credits, revision, created_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);

-- name: UpdateCreditPool :exec
UPDATE credit_pools SET limit_credits = $2, used_credits = $3, revision = $4 WHERE credit_pool_id = $1;

-- name: GetCreditEntry :one
SELECT * FROM credit_entries WHERE credit_pool_id = $1 AND operation_id = $2;

-- name: InsertCreditEntry :exec
INSERT INTO credit_entries (entry_id, operation_id, credit_pool_id, credits, created_at)
VALUES ($1, $2, $3, $4, $5);
