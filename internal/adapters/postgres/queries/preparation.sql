-- name: InsertQuestionSet :exec
INSERT INTO question_sets (
    question_set_id, operation_id, tenant_id, revision, round, questions, state, command_id, request_digest, asked_at, expires_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);

-- name: GetQuestionSet :one
SELECT * FROM question_sets WHERE question_set_id = $1;

-- name: LockQuestionSet :one
SELECT * FROM question_sets WHERE question_set_id = $1 FOR UPDATE;

-- name: GetQuestionSetByCommand :one
SELECT * FROM question_sets WHERE operation_id = $1 AND command_id = $2;

-- name: GetOpenQuestionSet :one
SELECT * FROM question_sets WHERE operation_id = $1 AND state = 'open';

-- name: ListQuestionSets :many
SELECT * FROM question_sets WHERE operation_id = $1 ORDER BY round;

-- name: UpdateQuestionSetState :exec
UPDATE question_sets SET state = $2 WHERE question_set_id = $1;

-- name: InsertAnswer :exec
INSERT INTO answers (
    answer_id, operation_id, tenant_id, actor_id, question_set_id, question_set_revision, transfer_id, digest, handle, update_id,
    command_id, request_digest, relay_state, accepted_at, relayed_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);

-- name: GetAnswerByCommand :one
SELECT * FROM answers WHERE tenant_id = $1 AND command_id = $2;

-- name: GetAnswer :one
SELECT * FROM answers WHERE answer_id = $1;

-- name: LockAnswer :one
SELECT * FROM answers WHERE answer_id = $1 FOR UPDATE;

-- name: GetAnswerByQuestionSet :one
SELECT * FROM answers WHERE question_set_id = $1 AND question_set_revision = $2;

-- name: ListAnswers :many
SELECT * FROM answers WHERE operation_id = $1 ORDER BY accepted_at;

-- name: ListAnswerRelayPending :many
SELECT * FROM answers WHERE relay_state IN ('pending', 'sent') ORDER BY accepted_at LIMIT $1;

-- name: UpdateAnswerRelay :exec
UPDATE answers SET relay_state = $2, relayed_at = $3 WHERE answer_id = $1;

-- name: InsertBrief :exec
INSERT INTO briefs (
    brief_id, operation_id, tenant_id, revision, transfer_id, digest, handle, requirements_digest, source_revisions, brand_digests,
    asset_digests, state, command_id, request_digest, frozen_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);

-- name: GetBrief :one
SELECT * FROM briefs WHERE brief_id = $1;

-- name: GetBriefByCommand :one
SELECT * FROM briefs WHERE operation_id = $1 AND command_id = $2;

-- name: GetCurrentBrief :one
SELECT * FROM briefs WHERE operation_id = $1 AND state = 'current';

-- name: CountBriefs :one
SELECT count(*) FROM briefs WHERE operation_id = $1;

-- name: SupersedeBriefs :exec
UPDATE briefs SET state = 'superseded' WHERE operation_id = $1 AND state = 'current';
