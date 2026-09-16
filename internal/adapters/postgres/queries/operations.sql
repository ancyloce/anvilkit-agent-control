-- name: InsertOperation :exec
INSERT INTO operations (
    operation_id, tenant_id, project_id, actor_id, command_id, kind, profile_id, subject_digest, brief_id, source_revision,
    semantic_digest, lifecycle, phase, control_state, cleanup_state, finance_state, failure_code, revision, next_event_seq,
    execution_epoch, recovery_epoch, deadline, intake_state, intake_version, relay_state, relay_run_id, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19,
    $20, $21, $22, $23, $24, $25, $26, $27, $28
);

-- name: GetOperationByCommand :one
SELECT * FROM operations WHERE tenant_id = $1 AND command_id = $2;

-- name: GetOperationScoped :one
SELECT * FROM operations WHERE operation_id = $1 AND tenant_id = $2;

-- name: LockOperation :one
SELECT * FROM operations WHERE operation_id = $1 FOR UPDATE;

-- name: UpdateOperation :exec
UPDATE operations SET
    lifecycle = $2, phase = $3, control_state = $4, cleanup_state = $5, finance_state = $6, failure_code = $7,
    revision = $8, next_event_seq = $9, execution_epoch = $10, recovery_epoch = $11,
    intake_state = $12, intake_version = $13, relay_state = $14, relay_run_id = $15, updated_at = $16
WHERE operation_id = $1;

-- name: InsertOperationEvent :exec
INSERT INTO operation_events (operation_id, event_seq, transition_id, event_type, revision, payload, occurred_at)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: ListOperationEvents :many
SELECT * FROM operation_events WHERE operation_id = $1 AND event_seq > $2 ORDER BY event_seq LIMIT $3;

-- name: ListIntakePending :many
SELECT * FROM operations WHERE intake_state = 'pending' AND created_at < $1 ORDER BY created_at LIMIT $2;

-- name: ListRelayPending :many
SELECT * FROM operations
WHERE (relay_state = 'pending' AND intake_state = 'confirmed') OR relay_state = 'cancel_pending'
ORDER BY created_at LIMIT $1;

-- name: GetCommand :one
SELECT * FROM operation_commands WHERE tenant_id = $1 AND command_id = $2;

-- name: InsertCommand :exec
INSERT INTO operation_commands (
    tenant_id, command_id, operation_id, actor_id, kind, expected_revision, request_digest, target_definition_activation,
    outcome, reason_code, operation_revision, accepted_at, settled_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);

-- name: SettlePendingCommands :exec
UPDATE operation_commands SET outcome = $3, operation_revision = $4, settled_at = $5
WHERE operation_id = $1 AND kind = $2 AND outcome = 'pending';
