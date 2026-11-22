-- +goose Up
-- DEC-070 (design 0001 D4a): a reply's text is committed in batches while it
-- streams, and each conversation keeps a durable event log that the API serves
-- as SSE. A batch is one range of the Model Proxy's original frames (provider
-- sequences from 0, contiguous; design §8.2); its text, when it has any, is a
-- chunk. The reply's assistant message id and sequence are reserved on the
-- turn at its first text and the message row is inserted once, at the seal,
-- so conversation_messages stays append-only for the runtime role. Event rows
-- hold identities, sequences and states, never a body: a message.delta
-- references its chunk. Both new tables are append-only for the runtime role.
-- Additive: a Control release without this migration ignores the new tables
-- and columns.

ALTER TABLE conversations
    ADD COLUMN next_event_seq bigint NOT NULL DEFAULT 1 CHECK (next_event_seq >= 1);

ALTER TABLE conversation_messages
    -- The text's version: 1 for a user message and a sealed reply stored
    -- whole; the last chunk's number for a reply sealed from its batches.
    ADD COLUMN content_revision bigint NOT NULL DEFAULT 1 CHECK (content_revision >= 1),
    -- The last committed chunk the text covers; 0 without chunks.
    ADD COLUMN through_chunk_seq bigint NOT NULL DEFAULT 0 CHECK (through_chunk_seq >= 0);

ALTER TABLE conversation_turns
    -- The Model Proxy's original stream of the reply call, fixed by the first
    -- batch, and the last committed provider sequence (NULL before any: the
    -- first batch starts at 0).
    ADD COLUMN original_stream_id text CHECK (char_length(original_stream_id) BETWEEN 1 AND 128),
    ADD COLUMN last_provider_seq bigint CHECK (last_provider_seq >= 0),
    ADD COLUMN last_batch_seq bigint NOT NULL DEFAULT 0 CHECK (last_batch_seq >= 0),
    ADD COLUMN last_chunk_seq bigint NOT NULL DEFAULT 0 CHECK (last_chunk_seq >= 0),
    ADD COLUMN committed_bytes bigint NOT NULL DEFAULT 0 CHECK (committed_bytes BETWEEN 0 AND 32768),
    -- The reply's assistant message, reserved at its first text.
    ADD COLUMN reserved_message_id text,
    ADD COLUMN reserved_message_seq bigint CHECK (reserved_message_seq >= 1),
    ADD CONSTRAINT conversation_turns_stream_check CHECK ((original_stream_id IS NULL) = (last_provider_seq IS NULL)),
    ADD CONSTRAINT conversation_turns_reserved_check CHECK ((reserved_message_id IS NULL) = (reserved_message_seq IS NULL)),
    ADD CONSTRAINT conversation_turns_chunks_check CHECK ((last_chunk_seq = 0) = (reserved_message_id IS NULL));

-- A streaming reply's message is read by its reserved id (GetMessage).
CREATE UNIQUE INDEX conversation_turns_reserved_idx ON conversation_turns (reserved_message_id) WHERE reserved_message_id IS NOT NULL;

CREATE TABLE conversation_reply_chunks (
    turn_id              text NOT NULL REFERENCES conversation_turns (turn_id),
    tenant_id            text NOT NULL,
    -- From 1 per turn, one per accepted batch.
    batch_seq            bigint NOT NULL CHECK (batch_seq >= 1),
    -- From 1 per turn for a batch with text; NULL for a textless batch.
    chunk_seq            bigint CHECK (chunk_seq >= 1),
    provider_from_seq    bigint NOT NULL CHECK (provider_from_seq >= 0),
    provider_through_seq bigint NOT NULL,
    source_frames_digest text NOT NULL CHECK (source_frames_digest ~ '^sha256:[0-9a-f]{64}$'),
    text                 text,
    text_digest          text CHECK (text_digest ~ '^sha256:[0-9a-f]{64}$'),
    created_at           timestamptz NOT NULL,
    PRIMARY KEY (turn_id, batch_seq),
    UNIQUE (turn_id, chunk_seq),
    UNIQUE (turn_id, provider_from_seq),
    CONSTRAINT conversation_reply_chunks_range_check CHECK (provider_through_seq >= provider_from_seq),
    CONSTRAINT conversation_reply_chunks_text_check CHECK (
        (chunk_seq IS NULL) = (text IS NULL)
        AND (text IS NULL) = (text_digest IS NULL)
        AND (text IS NULL OR octet_length(text) BETWEEN 1 AND 32768))
);

CREATE TABLE conversation_events (
    conversation_id text NOT NULL,
    tenant_id       text NOT NULL,
    event_seq       bigint NOT NULL CHECK (event_seq >= 1),
    event_type      text NOT NULL CHECK (event_type IN ('message.accepted', 'message.started', 'message.delta', 'message.completed',
        'message.failed', 'message.canceled', 'turn.changed', 'conversation.changed')),
    message_id      text,
    turn_id         text,
    -- A message.delta's chunk, whose text is served from conversation_reply_chunks.
    chunk_seq       bigint CHECK (chunk_seq >= 1),
    -- The event's identities, sequences and states; never a body.
    payload         jsonb NOT NULL,
    occurred_at     timestamptz NOT NULL,
    PRIMARY KEY (conversation_id, event_seq),
    FOREIGN KEY (tenant_id, conversation_id) REFERENCES conversations (tenant_id, conversation_id),
    CONSTRAINT conversation_events_delta_check CHECK ((event_type = 'message.delta') = (chunk_seq IS NOT NULL AND turn_id IS NOT NULL))
);

GRANT SELECT, INSERT ON conversation_reply_chunks, conversation_events TO anvilkit_control_app;

-- +goose Down
REVOKE ALL ON conversation_reply_chunks, conversation_events FROM anvilkit_control_app;
DROP TABLE conversation_events;
DROP TABLE conversation_reply_chunks;
DROP INDEX conversation_turns_reserved_idx;
ALTER TABLE conversation_turns
    DROP CONSTRAINT conversation_turns_chunks_check,
    DROP CONSTRAINT conversation_turns_reserved_check,
    DROP CONSTRAINT conversation_turns_stream_check,
    DROP COLUMN reserved_message_seq,
    DROP COLUMN reserved_message_id,
    DROP COLUMN committed_bytes,
    DROP COLUMN last_chunk_seq,
    DROP COLUMN last_batch_seq,
    DROP COLUMN last_provider_seq,
    DROP COLUMN original_stream_id;
ALTER TABLE conversation_messages
    DROP COLUMN through_chunk_seq,
    DROP COLUMN content_revision;
ALTER TABLE conversations
    DROP COLUMN next_event_seq;
