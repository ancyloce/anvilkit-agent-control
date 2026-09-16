-- name: GetAttemptByCommand :one
SELECT * FROM attempts WHERE operation_id = $1 AND command_id = $2;

-- name: LockAttempt :one
SELECT * FROM attempts WHERE attempt_id = $1 FOR UPDATE;

-- name: CountAttempts :one
SELECT count(*) FROM attempts WHERE operation_id = $1 AND step_id = $2 AND visit_ordinal = $3;

-- name: HasOpenAttempt :one
SELECT EXISTS (SELECT 1 FROM attempts WHERE operation_id = $1 AND state <> 'closed');

-- name: InsertAttempt :exec
INSERT INTO attempts (
    attempt_id, operation_id, tenant_id, step_id, visit_ordinal, attempt_ordinal, profile_id, execution_epoch,
    command_id, request_digest, state, outcome, cleanup_state, failure_code, accepted_stage_id, deadline, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18);

-- name: UpdateAttempt :exec
UPDATE attempts SET state = $2, outcome = $3, cleanup_state = $4, failure_code = $5, accepted_stage_id = $6, updated_at = $7
WHERE attempt_id = $1;

-- name: GetLaunchByCommand :one
SELECT * FROM launches WHERE attempt_id = $1 AND command_id = $2;

-- name: GetLaunchByKey :one
SELECT * FROM launches WHERE backend = $1 AND launch_key = $2;

-- name: LockLaunch :one
SELECT * FROM launches WHERE launch_id = $1 FOR UPDATE;

-- name: InsertLaunch :exec
INSERT INTO launches (
    launch_id, attempt_id, operation_id, launch_key, backend, profile_id, image_digest, execution_epoch, launch_epoch,
    deadline, command_id, request_digest, inventory_state, inventory_version, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16);

-- name: UpdateLaunchInventory :exec
UPDATE launches SET inventory_state = $2, inventory_version = $3, updated_at = $4 WHERE launch_id = $1;

-- name: GetInstanceByPod :one
SELECT * FROM physical_instances WHERE backend = $1 AND pod_uid = $2;

-- name: LockInstance :one
SELECT * FROM physical_instances WHERE instance_id = $1 FOR UPDATE;

-- name: HasCurrentInstance :one
SELECT EXISTS (SELECT 1 FROM physical_instances WHERE attempt_id = $1 AND is_current);

-- name: InsertInstance :exec
INSERT INTO physical_instances (
    instance_id, attempt_id, launch_id, launch_key, backend, job_uid, pod_uid, image_digest, launch_epoch, phase, exit_code,
    is_current, registered_at, observed_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);

-- name: UpdateInstanceObservation :exec
UPDATE physical_instances SET phase = $2, exit_code = $3, observed_at = $4 WHERE instance_id = $1;

-- name: GetStageByAttempt :one
SELECT * FROM stage_manifests WHERE attempt_id = $1 AND phase_ordinal = 1;

-- name: InsertStage :exec
INSERT INTO stage_manifests (
    stage_id, attempt_id, instance_id, operation_id, phase_ordinal, profile_id, verdict, failure_code, result_digest,
    result_manifest, result_manifest_bytes, observer_identity, command_id, request_digest, accepted_at, execution_epoch, recovery_epoch
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);

-- name: GetAttempt :one
SELECT * FROM attempts WHERE attempt_id = $1;

-- name: GetLaunch :one
SELECT * FROM launches WHERE launch_id = $1;
