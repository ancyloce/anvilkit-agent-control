-- +goose Up
-- P13 (DD-01 §3–§4, DD-02 §2–§3, contracts.md §2): the durable human-input
-- records of a Preparation (question sets with their immutable absolute
-- expiry, one answer per question set revision with its Update relay
-- intent, the frozen brief) and the admission facts of a Generation (the
-- execution permit that sets the active deadline once, the confirmed lease
-- record, the funding occurrence, the reviewed definition activation) plus
-- the relay intent of the tracked control commands (hold, resume,
-- change_definition) that reach the Workflow as Updates. Nothing here
-- rewrites an applied migration: operations and operation_commands gain
-- nullable or defaulted columns; the tables are new.

ALTER TABLE operations
    ADD COLUMN active_deadline       timestamptz,
    ADD COLUMN lease_state           text NOT NULL DEFAULT 'none' CHECK (lease_state IN ('none', 'held', 'lost', 'released')),
    ADD COLUMN lease_id              text,
    ADD COLUMN lease_fence           bigint,
    ADD COLUMN lease_expires_at      timestamptz,
    ADD COLUMN lease_occurrence      bigint NOT NULL DEFAULT 0,
    ADD COLUMN definition_activation text NOT NULL DEFAULT '',
    ADD COLUMN prompt_transfer_id    text,
    ADD COLUMN prompt_digest         text CHECK (prompt_digest IS NULL OR prompt_digest ~ '^sha256:[0-9a-f]{64}$'),
    ADD COLUMN brand_references      jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN asset_references      jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN candidate_effect_id   text;

ALTER TABLE operation_commands
    ADD COLUMN relay_state text NOT NULL DEFAULT 'none' CHECK (relay_state IN ('none', 'pending', 'sent', 'settled'));
CREATE INDEX operation_commands_relay_pending_idx ON operation_commands (accepted_at) WHERE relay_state IN ('pending', 'sent');

-- A grouped clarification round: questions, when it was asked and the
-- immutable absolute expiry of the wait. One open set per operation.
CREATE TABLE question_sets (
    question_set_id text PRIMARY KEY,
    operation_id    text NOT NULL REFERENCES operations (operation_id),
    tenant_id       text NOT NULL,
    revision        bigint NOT NULL CHECK (revision >= 1),
    round           bigint NOT NULL CHECK (round >= 1),
    questions       jsonb NOT NULL,
    state           text NOT NULL CHECK (state IN ('open', 'answered', 'expired', 'superseded')),
    command_id      text NOT NULL,
    request_digest  text NOT NULL,
    asked_at        timestamptz NOT NULL,
    expires_at      timestamptz NOT NULL,
    UNIQUE (operation_id, round),
    UNIQUE (operation_id, command_id)
);
CREATE UNIQUE INDEX question_sets_open_idx ON question_sets (operation_id) WHERE state = 'open';

-- One accepted answer per question set revision, committed with the
-- identity of the tracked Update that relays it.
CREATE TABLE answers (
    answer_id             text PRIMARY KEY,
    operation_id          text NOT NULL REFERENCES operations (operation_id),
    tenant_id             text NOT NULL,
    actor_id              text NOT NULL,
    question_set_id       text NOT NULL REFERENCES question_sets (question_set_id),
    question_set_revision bigint NOT NULL,
    transfer_id           text NOT NULL,
    digest                text NOT NULL CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    handle                text NOT NULL,
    update_id             text NOT NULL,
    command_id            text NOT NULL,
    request_digest        text NOT NULL,
    relay_state           text NOT NULL CHECK (relay_state IN ('pending', 'sent', 'applied', 'rejected')),
    accepted_at           timestamptz NOT NULL,
    relayed_at            timestamptz,
    UNIQUE (tenant_id, command_id),
    UNIQUE (question_set_id, question_set_revision)
);
CREATE INDEX answers_relay_pending_idx ON answers (accepted_at) WHERE relay_state IN ('pending', 'sent');

-- The frozen brief: the brief artifact, the requirements digest and the
-- exact inputs it binds. A new brief of the same preparation supersedes
-- the previous one; a superseded brief starts no generation.
CREATE TABLE briefs (
    brief_id            text PRIMARY KEY,
    operation_id        text NOT NULL REFERENCES operations (operation_id),
    tenant_id           text NOT NULL,
    revision            bigint NOT NULL CHECK (revision >= 1),
    transfer_id         text NOT NULL,
    digest              text NOT NULL CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    handle              text NOT NULL,
    requirements_digest text NOT NULL CHECK (requirements_digest ~ '^sha256:[0-9a-f]{64}$'),
    source_revisions    jsonb NOT NULL DEFAULT '[]'::jsonb,
    brand_digests       jsonb NOT NULL DEFAULT '[]'::jsonb,
    asset_digests       jsonb NOT NULL DEFAULT '[]'::jsonb,
    state               text NOT NULL CHECK (state IN ('current', 'superseded')),
    command_id          text NOT NULL,
    request_digest      text NOT NULL,
    frozen_at           timestamptz NOT NULL,
    UNIQUE (operation_id, revision),
    UNIQUE (operation_id, command_id)
);
CREATE UNIQUE INDEX briefs_current_idx ON briefs (operation_id) WHERE state = 'current';

-- The funding occurrence of an operation (DD-02 §3): the reviewed amount
-- allocated once under the operation identity; the allocations themselves
-- are the existing rows.
CREATE TABLE fundings (
    operation_id   text PRIMARY KEY REFERENCES operations (operation_id),
    command_id     text NOT NULL,
    request_digest text NOT NULL,
    currency       text NOT NULL,
    amount         bigint NOT NULL CHECK (amount >= 0),
    funded_at      timestamptz NOT NULL
);

GRANT SELECT, INSERT, UPDATE ON question_sets, answers, briefs, fundings TO anvilkit_control_app;

-- +goose Down
REVOKE ALL ON question_sets, answers, briefs, fundings FROM anvilkit_control_app;
DROP TABLE fundings;
DROP TABLE briefs;
DROP TABLE answers;
DROP TABLE question_sets;
DROP INDEX operation_commands_relay_pending_idx;
ALTER TABLE operation_commands DROP COLUMN relay_state;
ALTER TABLE operations
    DROP COLUMN active_deadline, DROP COLUMN lease_state, DROP COLUMN lease_id, DROP COLUMN lease_fence, DROP COLUMN lease_expires_at,
    DROP COLUMN lease_occurrence, DROP COLUMN definition_activation, DROP COLUMN prompt_transfer_id, DROP COLUMN prompt_digest,
    DROP COLUMN brand_references, DROP COLUMN asset_references, DROP COLUMN candidate_effect_id;
