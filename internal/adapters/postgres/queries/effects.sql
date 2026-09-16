-- Business-write obligations, their single-use permit, durable outcome
-- observations and evidence-bound dispositions (DD-02 §5, DD-06 §3, P07).
-- Lock ranks: operation (2), attempt (3), effect (6); the application
-- performs the inventory write between its two transactions, never under
-- these locks.

-- name: GetEffectByCommand :one
SELECT * FROM effect_intents WHERE tenant_id = $1 AND command_id = $2;

-- name: GetEffectByOccurrence :one
SELECT * FROM effect_intents WHERE operation_id = $1 AND kind = $2 AND occurrence = $3;

-- name: GetEffect :one
SELECT * FROM effect_intents WHERE effect_id = $1;

-- name: LockEffect :one
SELECT * FROM effect_intents WHERE effect_id = $1 FOR UPDATE;

-- name: InsertEffect :exec
INSERT INTO effect_intents (
    effect_id, operation_id, kind, occurrence, canonical_subject, request_digest, expected_revision, state, outcome_ref, query_ref,
    inventory_state, inventory_version, created_at, updated_at, tenant_id, attempt_id, command_id, owner, execution_epoch, recovery_epoch,
    lease_id, lease_fence, lease_expires_at, outcome, denial_code, deadline, observed_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
    $21, $22, $23, $24, $25, $26, $27
);

-- name: UpdateEffect :exec
UPDATE effect_intents SET state = $2, outcome = $3, denial_code = $4, outcome_ref = $5, query_ref = $6, inventory_state = $7, inventory_version = $8,
    recovery_epoch = $9, observed_at = $10, updated_at = $11
WHERE effect_id = $1;

-- name: HasUnresolvedEffect :one
SELECT EXISTS (SELECT 1 FROM effect_intents WHERE operation_id = $1 AND state IN ('permitted', 'unknown'));

-- name: GetEffectObservation :one
SELECT * FROM effect_observations WHERE effect_id = $1 AND source = $2 AND sequence = $3;

-- name: InsertEffectObservation :exec
INSERT INTO effect_observations (observation_id, effect_id, source, sequence, outcome, receipt_digest, native_reference, observed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: GetDisposition :one
SELECT * FROM obligation_dispositions WHERE class = $1 AND obligation_id = $2;

-- name: GetDispositionByCommand :one
SELECT * FROM obligation_dispositions WHERE tenant_id = $1 AND command_id = $2;

-- name: InsertDisposition :exec
INSERT INTO obligation_dispositions (
    disposition_id, class, obligation_id, tenant_id, run_id, recovery_epoch, command_id, actor_id, request_digest, decision,
    evidence_ref, evidence_digest, reason, decided_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);
