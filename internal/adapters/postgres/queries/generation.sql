-- name: LockResourcePool :one
SELECT * FROM resource_pools WHERE pool_id = $1 FOR UPDATE;

-- name: UpsertResourcePool :exec
INSERT INTO resource_pools (pool_id, class, capacity, reserved_control) VALUES ($1, $2, $3, $4)
ON CONFLICT (pool_id) DO UPDATE SET class = EXCLUDED.class, capacity = EXCLUDED.capacity, reserved_control = EXCLUDED.reserved_control;

-- name: CountActivePermits :one
SELECT count(*) FROM permits WHERE pool_id = $1 AND state = 'active';

-- name: CountActivePermitsBefore :one
SELECT count(*) FROM permits WHERE pool_id = $1 AND state = 'active' AND granted_at < $2;

-- name: GetActivePermit :one
SELECT * FROM permits WHERE pool_id = $1 AND owner_kind = $2 AND owner_id = $3 AND state = 'active';

-- name: GetPermitByOwner :one
SELECT * FROM permits WHERE owner_kind = $1 AND owner_id = $2 ORDER BY granted_at DESC LIMIT 1;

-- name: InsertPermit :exec
INSERT INTO permits (permit_id, pool_id, owner_kind, owner_id, fence_epoch, state, granted_at, released_at, release_evidence)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: ReleasePermits :exec
UPDATE permits SET state = 'released', released_at = $3, release_evidence = $4
WHERE owner_kind = $1 AND owner_id = $2 AND state = 'active';

-- name: GetFunding :one
SELECT * FROM fundings WHERE operation_id = $1;

-- name: InsertFunding :exec
INSERT INTO fundings (operation_id, command_id, request_digest, currency, amount, funded_at) VALUES ($1, $2, $3, $4, $5, $6);

-- name: ListStagesByOperation :many
SELECT * FROM stage_manifests WHERE operation_id = $1 ORDER BY accepted_at;

-- name: GetStageArtifactByTransfer :one
SELECT sa.* FROM stage_artifacts sa JOIN stage_manifests sm ON sm.stage_id = sa.stage_id
WHERE sa.transfer_id = $1 AND sm.operation_id = $2 LIMIT 1;

-- name: GetOperationSettlement :one
SELECT * FROM operation_settlements WHERE operation_id = $1;

-- name: InsertOperationSettlement :exec
INSERT INTO operation_settlements (operation_id, command_id, request_digest, outcome, failure_code, phase)
VALUES ($1, $2, $3, $4, $5, $6);
