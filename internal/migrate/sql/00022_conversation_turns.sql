-- +goose Up
-- DEC-068 (design 0001 D3): assistant replies. Each accepted user message of a
-- conversation with replies enabled opens one turn, answered by one chat_turn
-- operation. A conversation has at most one open turn (design §4.3): the
-- partial unique index is the slot, so a second message is refused before
-- anything is stored. The reply writer (activity key, temporal attempt,
-- invocation, epoch) fences a late Activity (design §7.2). Reply text is
-- stored as an assistant message in conversation_messages, which the runtime
-- role still only inserts. Turns are rank-1 rows, locked through their
-- conversation. Additive, apart from the operation kind's value list, which
-- gains chat_turn; a Control release without this migration has no turn.

ALTER TABLE operations DROP CONSTRAINT operations_kind_check;
ALTER TABLE operations ADD CONSTRAINT operations_kind_check
    CHECK (kind IN ('preparation', 'generation', 'refinement', 'preview_build', 'release', 'local_check', 'chat_turn'));

CREATE TABLE conversation_turns (
    turn_id              text PRIMARY KEY,
    tenant_id            text NOT NULL,
    conversation_id      text NOT NULL,
    input_message_id     text NOT NULL UNIQUE REFERENCES conversation_messages (message_id),
    input_message_seq    bigint NOT NULL CHECK (input_message_seq >= 1),
    operation_id         text NOT NULL UNIQUE REFERENCES operations (operation_id),
    state                text NOT NULL CHECK (state IN ('queued', 'running', 'completed', 'failed', 'cancel_pending', 'canceled', 'reconciling')),
    content_status       text NOT NULL CHECK (content_status IN ('complete', 'partial', 'unknown')),
    sender_status        text NOT NULL CHECK (sender_status IN ('not_started', 'active', 'stopped', 'unknown')),
    assistant_message_id text UNIQUE REFERENCES conversation_messages (message_id),
    failure_code         text CHECK (char_length(failure_code) BETWEEN 1 AND 64),
    activity_key         text CHECK (char_length(activity_key) BETWEEN 1 AND 256),
    temporal_attempt     bigint NOT NULL DEFAULT 0 CHECK (temporal_attempt >= 0),
    invocation_id        text,
    writer_epoch         bigint NOT NULL DEFAULT 0 CHECK (writer_epoch >= 0),
    outcome_command_id   text,
    outcome_digest       text CHECK (outcome_digest ~ '^sha256:[0-9a-f]{64}$'),
    created_at           timestamptz NOT NULL,
    updated_at           timestamptz NOT NULL,
    FOREIGN KEY (tenant_id, conversation_id) REFERENCES conversations (tenant_id, conversation_id),
    -- A claimed writer names its activity and invocation; an unclaimed one none.
    CONSTRAINT conversation_turns_writer_check CHECK ((writer_epoch = 0) = (activity_key IS NULL AND invocation_id IS NULL AND temporal_attempt = 0)),
    -- Only a completed turn has complete content; a sealed turn names its command.
    CONSTRAINT conversation_turns_complete_check CHECK ((state = 'completed') = (content_status = 'complete')),
    CONSTRAINT conversation_turns_sealed_check CHECK (
        state NOT IN ('completed', 'failed', 'canceled')
        OR (outcome_command_id IS NULL) = (outcome_digest IS NULL))
);
CREATE UNIQUE INDEX conversation_turns_one_open_idx ON conversation_turns (conversation_id)
    WHERE state IN ('queued', 'running', 'cancel_pending', 'reconciling');

GRANT SELECT, INSERT, UPDATE ON conversation_turns TO anvilkit_control_app;

-- +goose Down
REVOKE ALL ON conversation_turns FROM anvilkit_control_app;
DROP TABLE conversation_turns;
-- chat_turn operations keep their rows (every attempt, event and cost refers
-- to them); the restored list binds new rows only.
ALTER TABLE operations DROP CONSTRAINT operations_kind_check;
ALTER TABLE operations ADD CONSTRAINT operations_kind_check
    CHECK (kind IN ('preparation', 'generation', 'refinement', 'preview_build', 'release', 'local_check')) NOT VALID;
