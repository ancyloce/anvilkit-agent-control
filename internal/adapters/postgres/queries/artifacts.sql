-- Scoped artifact transfers (DD-02 §6, P08). Lock ranks: operation (2),
-- attempt/instance (3), transfer (6, an accepted-result record). The
-- application reads the object outside every lock and revalidates under the
-- transfer lock before finalizing. object_key is Control's own locator and
-- never leaves the adapter or the application layer.

-- name: GetTransferByCommand :one
SELECT * FROM artifact_transfers WHERE tenant_id = $1 AND command_id = $2;

-- name: GetTransfer :one
SELECT * FROM artifact_transfers WHERE transfer_id = $1;

-- name: GetTransferByHandle :one
SELECT * FROM artifact_transfers WHERE handle = $1;

-- name: LockTransfer :one
SELECT * FROM artifact_transfers WHERE transfer_id = $1 FOR UPDATE;

-- name: InsertTransfer :exec
INSERT INTO artifact_transfers (
    transfer_id, tenant_id, operation_id, attempt_id, class, media_type, expected_digest, expected_size, handle, state,
    object_version, reason_code, command_id, request_digest, deadline, created_at, finalized_at,
    actor_id, execution_epoch, recovery_epoch, object_key, actual_size, actual_digest, finalize_command_id, finalize_request_digest, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17,
    $18, $19, $20, $21, $22, $23, $24, $25, $26
);

-- name: UpdateTransfer :exec
UPDATE artifact_transfers SET state = $2, object_version = $3, reason_code = $4, finalized_at = $5, actual_size = $6, actual_digest = $7,
    finalize_command_id = $8, finalize_request_digest = $9, updated_at = $10
WHERE transfer_id = $1;

-- name: InsertStageArtifact :exec
INSERT INTO stage_artifacts (stage_id, transfer_id, handle, class, digest, size_bytes, object_version) VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: ListStageArtifacts :many
SELECT * FROM stage_artifacts WHERE stage_id = $1 ORDER BY handle;
