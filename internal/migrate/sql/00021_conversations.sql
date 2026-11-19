-- +goose Up
-- DEC-067 (plan 0007 OD-8, design 0001 first slice): personal conversations
-- and their user messages. A conversation belongs to the verified actor who
-- created it, within that actor's tenant and project ('' when the principal
-- names no project); every read matches all three. Message bodies live here,
-- in Control's private tables, and nowhere else: no event, log or trace
-- carries them. The runtime role appends messages and never rewrites them,
-- so a stored user message cannot be overwritten. Conversation commands
-- deduplicate per (tenant_id, command_id) like every other Control command
-- family. Conversations are rank-1 rows, locked after the credit pools.
-- Additive: a Control release without this migration ignores the tables.

CREATE TABLE conversations (
    conversation_id  text PRIMARY KEY,
    tenant_id        text NOT NULL,
    project_id       text NOT NULL,
    owner_actor_id   text NOT NULL CHECK (owner_actor_id <> ''),
    status           text NOT NULL CHECK (status IN ('active', 'archived')),
    title            text CHECK (char_length(title) BETWEEN 1 AND 200),
    revision         bigint NOT NULL CHECK (revision >= 1),
    last_message_seq bigint NOT NULL DEFAULT 0 CHECK (last_message_seq >= 0),
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    UNIQUE (tenant_id, conversation_id)
);
CREATE INDEX conversations_owner_idx ON conversations (tenant_id, project_id, owner_actor_id, created_at DESC, conversation_id DESC);

CREATE TABLE conversation_messages (
    message_id            text PRIMARY KEY,
    tenant_id             text NOT NULL,
    conversation_id       text NOT NULL,
    message_seq           bigint NOT NULL CHECK (message_seq >= 1),
    role                  text NOT NULL CHECK (role IN ('user', 'assistant')),
    state                 text NOT NULL CHECK (state IN ('pending', 'streaming', 'completed', 'partial_failed', 'canceled', 'reconciling')),
    body                  text NOT NULL,
    content_digest        text NOT NULL CHECK (content_digest ~ '^sha256:[0-9a-f]{64}$'),
    -- The conversation's revision after this message (its receipt).
    conversation_revision bigint NOT NULL CHECK (conversation_revision >= 1),
    created_at            timestamptz NOT NULL,
    FOREIGN KEY (tenant_id, conversation_id) REFERENCES conversations (tenant_id, conversation_id),
    UNIQUE (conversation_id, message_seq),
    CONSTRAINT conversation_messages_user_check CHECK (role <> 'user' OR (state = 'completed' AND octet_length(body) BETWEEN 1 AND 32768))
);

CREATE TABLE conversation_commands (
    tenant_id       text NOT NULL,
    command_id      text NOT NULL,
    kind            text NOT NULL CHECK (kind IN ('create', 'update', 'append_message')),
    actor_id        text NOT NULL,
    request_digest  text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    conversation_id text NOT NULL,
    message_id      text REFERENCES conversation_messages (message_id),
    accepted_at     timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, command_id),
    FOREIGN KEY (tenant_id, conversation_id) REFERENCES conversations (tenant_id, conversation_id),
    CONSTRAINT conversation_commands_message_check CHECK ((kind = 'append_message') = (message_id IS NOT NULL))
);

GRANT SELECT, INSERT, UPDATE ON conversations TO anvilkit_control_app;
GRANT SELECT, INSERT ON conversation_messages, conversation_commands TO anvilkit_control_app;

-- +goose Down
REVOKE ALL ON conversations, conversation_messages, conversation_commands FROM anvilkit_control_app;
DROP TABLE conversation_commands;
DROP TABLE conversation_messages;
DROP TABLE conversations;
