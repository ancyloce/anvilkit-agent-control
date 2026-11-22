-- Conversations (DEC-067, migration 00021): personal conversations, their
-- user messages and the commands that created or changed them. Every read is
-- tenant-scoped here; the application matches project and owner. Lock rank
-- 1 (conversations, after the credit pools), then rank 6 (commands and
-- messages).

-- name: InsertConversation :exec
INSERT INTO conversations (conversation_id, tenant_id, project_id, owner_actor_id, status, title, revision, last_message_seq, created_at, updated_at, next_event_seq)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);

-- name: GetConversation :one
SELECT * FROM conversations WHERE conversation_id = $1 AND tenant_id = $2;

-- name: LockConversation :one
SELECT * FROM conversations WHERE conversation_id = $1 AND tenant_id = $2 FOR UPDATE;

-- name: UpdateConversation :exec
UPDATE conversations SET status = $2, title = $3, revision = $4, last_message_seq = $5, updated_at = $6, next_event_seq = $7
WHERE conversation_id = $1;

-- name: ListConversations :many
SELECT * FROM conversations
WHERE tenant_id = sqlc.arg(tenant_id)::text AND project_id = sqlc.arg(project_id)::text AND owner_actor_id = sqlc.arg(owner_actor_id)::text
  AND (sqlc.arg(status)::text = '' OR status = sqlc.arg(status)::text)
  AND (sqlc.arg(after_id)::text = '' OR (created_at, conversation_id) < (sqlc.arg(after_created_at)::timestamptz, sqlc.arg(after_id)::text))
ORDER BY created_at DESC, conversation_id DESC
LIMIT sqlc.arg(row_limit)::int;

-- name: InsertConversationMessage :exec
INSERT INTO conversation_messages (message_id, tenant_id, conversation_id, message_seq, role, state, body, content_digest, conversation_revision, created_at,
    content_revision, through_chunk_seq)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);

-- name: GetConversationMessage :one
SELECT * FROM conversation_messages WHERE message_id = $1 AND tenant_id = $2;

-- name: ListConversationMessages :many
SELECT * FROM conversation_messages
WHERE conversation_id = sqlc.arg(conversation_id)::text AND tenant_id = sqlc.arg(tenant_id)::text AND message_seq < sqlc.arg(before_seq)::bigint
ORDER BY message_seq DESC
LIMIT sqlc.arg(row_limit)::int;

-- name: GetConversationCommand :one
SELECT * FROM conversation_commands WHERE tenant_id = $1 AND command_id = $2;

-- name: InsertConversationCommand :exec
INSERT INTO conversation_commands (tenant_id, command_id, kind, actor_id, request_digest, conversation_id, message_id, accepted_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- Turns (DEC-068, migration 00022): one assistant reply per user message,
-- rank 1, read and written under the conversation's lock.

-- name: InsertConversationTurn :exec
INSERT INTO conversation_turns (turn_id, tenant_id, conversation_id, input_message_id, input_message_seq, operation_id, state, content_status, sender_status,
    assistant_message_id, failure_code, activity_key, temporal_attempt, invocation_id, writer_epoch, outcome_command_id, outcome_digest, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19);

-- name: GetConversationTurn :one
SELECT * FROM conversation_turns WHERE turn_id = $1 AND tenant_id = $2;

-- name: GetConversationTurnByInput :one
SELECT * FROM conversation_turns WHERE input_message_id = $1 AND tenant_id = $2;

-- name: GetConversationTurnByOperation :one
SELECT * FROM conversation_turns WHERE operation_id = $1 AND tenant_id = $2;

-- The turns answering the given messages of one conversation, through the
-- unique input_message_id index: a turn's context leaves out the inputs of
-- turns stopped before their send (DEC-069).
-- name: ListConversationTurnsByInputs :many
SELECT * FROM conversation_turns
WHERE tenant_id = sqlc.arg(tenant_id)::text AND conversation_id = sqlc.arg(conversation_id)::text
  AND input_message_id = ANY(sqlc.arg(input_message_ids)::text[]);

-- name: GetOpenConversationTurn :one
SELECT * FROM conversation_turns
WHERE conversation_id = $1 AND tenant_id = $2 AND state IN ('queued', 'running', 'cancel_pending', 'reconciling');

-- name: UpdateConversationTurn :exec
UPDATE conversation_turns SET state = $2, content_status = $3, sender_status = $4, assistant_message_id = $5, failure_code = $6, activity_key = $7,
    temporal_attempt = $8, invocation_id = $9, writer_epoch = $10, outcome_command_id = $11, outcome_digest = $12, updated_at = $13,
    original_stream_id = $14, last_provider_seq = $15, last_batch_seq = $16, last_chunk_seq = $17, committed_bytes = $18,
    reserved_message_id = $19, reserved_message_seq = $20
WHERE turn_id = $1;

-- name: GetConversationTurnByReservedMessage :one
SELECT * FROM conversation_turns WHERE reserved_message_id = $1 AND tenant_id = $2;

-- Reply batches and the conversation event log (DEC-070, migration 00023):
-- append-only, written under the conversation's lock.

-- name: InsertReplyChunk :exec
INSERT INTO conversation_reply_chunks (turn_id, tenant_id, batch_seq, chunk_seq, provider_from_seq, provider_through_seq, source_frames_digest, text, text_digest, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);

-- name: GetReplyChunkByFrom :one
SELECT * FROM conversation_reply_chunks WHERE turn_id = $1 AND provider_from_seq = $2;

-- name: ListReplyChunkTexts :many
SELECT chunk_seq, text FROM conversation_reply_chunks
WHERE turn_id = $1 AND chunk_seq IS NOT NULL
ORDER BY chunk_seq;

-- name: InsertConversationEvent :exec
INSERT INTO conversation_events (conversation_id, tenant_id, event_seq, event_type, message_id, turn_id, chunk_seq, payload, occurred_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- A message.delta's text is its chunk's, joined at read; the event row holds none.
-- name: ListConversationEvents :many
SELECT e.event_seq, e.event_type, e.message_id, e.turn_id, e.chunk_seq, e.payload, e.occurred_at, c.text AS chunk_text
FROM conversation_events e
LEFT JOIN conversation_reply_chunks c ON c.turn_id = e.turn_id AND c.chunk_seq = e.chunk_seq
WHERE e.conversation_id = sqlc.arg(conversation_id)::text AND e.tenant_id = sqlc.arg(tenant_id)::text AND e.event_seq > sqlc.arg(after_seq)::bigint
ORDER BY e.event_seq
LIMIT sqlc.arg(row_limit)::int;
