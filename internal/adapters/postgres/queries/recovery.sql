-- Recovery runs over a rollback window, their resumable enumeration
-- cursors, findings and the admission closure of a scope (DD-02 §5,
-- platform.md recovery gates, P07). The run row is locked (rank 1, before
-- any operation) for every phase change.

-- name: GetRecoveryRunByCommand :one
SELECT * FROM recovery_runs WHERE scope_key = $1 AND command_id = $2;

-- name: GetRecoveryRun :one
SELECT * FROM recovery_runs WHERE run_id = $1;

-- name: LockRecoveryRun :one
SELECT * FROM recovery_runs WHERE run_id = $1 FOR UPDATE;

-- name: MaxRecoveryEpoch :one
SELECT coalesce(max(recovery_epoch), 0)::bigint FROM recovery_runs;

-- name: InsertRecoveryRun :exec
INSERT INTO recovery_runs (
    run_id, scope_key, tenant_id, command_id, actor_id, request_digest, recovery_epoch, phase, window_start, window_end,
    skew_micros, reason, fenced_count, created_at, updated_at, reopened_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16);

-- name: UpdateRecoveryRun :exec
UPDATE recovery_runs SET phase = $2, fenced_count = $3, updated_at = $4, reopened_at = $5 WHERE run_id = $1;

-- name: ListRecoveryPending :many
SELECT * FROM recovery_runs WHERE phase = 'fenced' ORDER BY created_at LIMIT $1;

-- name: ListActiveOperations :many
SELECT * FROM operations
WHERE lifecycle IN ('accepted', 'running', 'waiting', 'reconciling', 'suspended') AND ($1::text = '*' OR tenant_id = $1)
ORDER BY operation_id;

-- name: GetRecoveryProgress :one
SELECT * FROM recovery_progress WHERE run_id = $1 AND class = $2;

-- name: UpsertRecoveryProgress :exec
INSERT INTO recovery_progress (run_id, class, cursor, complete, seen) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (run_id, class) DO UPDATE SET cursor = EXCLUDED.cursor, complete = EXCLUDED.complete, seen = EXCLUDED.seen;

-- name: ListRecoveryProgress :many
SELECT * FROM recovery_progress WHERE run_id = $1 ORDER BY class;

-- name: GetFinding :one
SELECT * FROM recovery_findings WHERE finding_id = $1;

-- name: LockFinding :one
SELECT * FROM recovery_findings WHERE finding_id = $1 FOR UPDATE;

-- name: GetFindingByObligation :one
SELECT * FROM recovery_findings WHERE run_id = $1 AND class = $2 AND obligation_id = $3;

-- name: InsertFinding :exec
INSERT INTO recovery_findings (
    finding_id, run_id, class, obligation_id, tenant_id, inventory_key, inventory_version, recorded_at, status, outcome, evidence_ref, detail, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);

-- name: UpdateFinding :exec
UPDATE recovery_findings SET status = $2, outcome = $3, evidence_ref = $4, detail = $5, updated_at = $6 WHERE finding_id = $1;

-- name: ListFindings :many
-- Keyset pagination over the fixed (class, obligation_id) order: the cursor
-- is the key of the last finding of the previous page, so every finding is
-- reached whatever the state of the ones before it.
SELECT * FROM recovery_findings
WHERE run_id = $1 AND ($2::text = '' OR status = $2) AND (class, obligation_id) > ($3::text, $4::text)
ORDER BY class, obligation_id LIMIT $5;

-- name: CountUnsettledFindings :one
SELECT count(*) FROM recovery_findings WHERE run_id = $1 AND status NOT IN ('present', 'resolved', 'disposed', 'outside_scope');

-- name: GetAdmissionClosure :one
SELECT * FROM admission_closures WHERE scope_key = $1;

-- name: IsAdmissionClosed :one
SELECT EXISTS (SELECT 1 FROM admission_closures WHERE scope_key IN ('*', $1) AND reopened_at IS NULL);

-- name: UpsertAdmissionClosure :exec
INSERT INTO admission_closures (scope_key, run_id, closed_at, reopened_at) VALUES ($1, $2, $3, NULL)
ON CONFLICT (scope_key) DO UPDATE SET run_id = EXCLUDED.run_id, closed_at = EXCLUDED.closed_at, reopened_at = NULL;

-- name: ReopenAdmission :exec
UPDATE admission_closures SET reopened_at = $3 WHERE scope_key = $1 AND run_id = $2 AND reopened_at IS NULL;

-- name: HasOpenRecoveryFor :one
SELECT EXISTS (SELECT 1 FROM admission_closures WHERE scope_key = $1 AND reopened_at IS NULL);
