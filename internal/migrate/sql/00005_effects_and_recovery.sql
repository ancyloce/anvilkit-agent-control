-- +goose Up
-- P07 (DD-02 §5, DD-06 §3, platform.md recovery gates): the business-write
-- obligation with its single-use permit, durable outcome observations,
-- evidence-bound operator dispositions, recovery runs over a rollback
-- window and the admission closure of a scope under recovery. Nothing here
-- rewrites an applied migration; effect_intents gains the columns the permit
-- chain binds and the states of its machine.

ALTER TABLE effect_intents
    ADD COLUMN tenant_id        text NOT NULL DEFAULT '',
    ADD COLUMN attempt_id       text,
    ADD COLUMN command_id       text NOT NULL DEFAULT '',
    ADD COLUMN owner            text NOT NULL DEFAULT '',
    ADD COLUMN execution_epoch  bigint NOT NULL DEFAULT 1,
    ADD COLUMN recovery_epoch   bigint NOT NULL DEFAULT 1,
    ADD COLUMN lease_id         text,
    ADD COLUMN lease_fence      bigint,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN outcome          text CHECK (outcome IN ('succeeded', 'failed', 'unknown')),
    ADD COLUMN denial_code      text,
    ADD COLUMN deadline         timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN observed_at      timestamptz;
ALTER TABLE effect_intents DROP CONSTRAINT effect_intents_state_check;
ALTER TABLE effect_intents ADD CONSTRAINT effect_intents_state_check
    CHECK (state IN ('prepared', 'permitted', 'sent', 'succeeded', 'failed', 'unknown', 'confirmed_not_sent', 'denied'));
-- Same command identity reenters the same effect; one obligation per
-- (operation, kind, occurrence) is the existing constraint.
CREATE UNIQUE INDEX effect_intents_command_idx ON effect_intents (tenant_id, command_id) WHERE command_id <> '';
CREATE INDEX effect_intents_unresolved_idx ON effect_intents (created_at) WHERE state IN ('permitted', 'unknown');

CREATE TABLE effect_observations (
    observation_id   text PRIMARY KEY,
    effect_id        text NOT NULL REFERENCES effect_intents (effect_id),
    source           text NOT NULL,
    sequence         bigint NOT NULL CHECK (sequence >= 0),
    outcome          text NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'unknown')),
    receipt_digest   text NOT NULL DEFAULT '',
    native_reference text NOT NULL DEFAULT '',
    observed_at      timestamptz NOT NULL,
    UNIQUE (effect_id, source, sequence)
);

-- Operator dispositions are evidence-bound decisions about one obligation
-- each; the evidence object is verified before the row is written.
CREATE TABLE obligation_dispositions (
    disposition_id  text PRIMARY KEY,
    class           text NOT NULL CHECK (class IN ('intake', 'job-launch', 'model-dispatch', 'tool-dispatch', 'business-write')),
    obligation_id   text NOT NULL,
    tenant_id       text NOT NULL,
    run_id          text,
    recovery_epoch  bigint NOT NULL,
    command_id      text NOT NULL,
    actor_id        text NOT NULL,
    request_digest  text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    decision        text NOT NULL CHECK (decision IN ('confirm_not_sent', 'resolve_succeeded', 'resolve_failed', 'retain_exposure')),
    evidence_ref    text NOT NULL,
    evidence_digest text NOT NULL CHECK (evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
    reason          text NOT NULL DEFAULT '',
    decided_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (class, obligation_id),
    UNIQUE (tenant_id, command_id)
);

CREATE TABLE recovery_runs (
    run_id          text PRIMARY KEY,
    scope_key       text NOT NULL,
    tenant_id       text NOT NULL DEFAULT '',
    command_id      text NOT NULL,
    actor_id        text NOT NULL,
    request_digest  text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    recovery_epoch  bigint NOT NULL CHECK (recovery_epoch >= 1),
    phase           text NOT NULL CHECK (phase IN ('fenced', 'enumerating', 'reconciling', 'restricted', 'reopened')),
    window_start    timestamptz NOT NULL,
    window_end      timestamptz NOT NULL,
    skew_micros     bigint NOT NULL DEFAULT 0 CHECK (skew_micros >= 0),
    reason          text NOT NULL DEFAULT '',
    fenced_count    bigint NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    reopened_at     timestamptz,
    UNIQUE (scope_key, command_id),
    CHECK (window_end > window_start)
);

CREATE TABLE recovery_progress (
    run_id   text NOT NULL REFERENCES recovery_runs (run_id),
    class    text NOT NULL,
    cursor   text NOT NULL DEFAULT '',
    complete boolean NOT NULL DEFAULT false,
    seen     bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, class)
);

CREATE TABLE recovery_findings (
    finding_id        text PRIMARY KEY,
    run_id            text NOT NULL REFERENCES recovery_runs (run_id),
    class             text NOT NULL,
    obligation_id     text NOT NULL,
    tenant_id         text NOT NULL,
    inventory_key     text NOT NULL,
    inventory_version text NOT NULL,
    recorded_at       timestamptz NOT NULL,
    status            text NOT NULL CHECK (status IN ('present', 'missing', 'restored', 'resolved', 'unresolved', 'disposed', 'outside_scope')),
    outcome           text NOT NULL DEFAULT '',
    evidence_ref      text NOT NULL DEFAULT '',
    detail            text NOT NULL DEFAULT '',
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, class, obligation_id)
);
CREATE INDEX recovery_findings_status_idx ON recovery_findings (run_id, status);

-- One closure row per scope: while reopened_at is NULL the scope admits
-- nothing new (intake, attempts, launches, sends, business writes).
CREATE TABLE admission_closures (
    scope_key   text PRIMARY KEY,
    run_id      text NOT NULL,
    closed_at   timestamptz NOT NULL DEFAULT now(),
    reopened_at timestamptz
);

GRANT SELECT, INSERT, UPDATE ON effect_observations, obligation_dispositions, recovery_runs, recovery_progress, recovery_findings, admission_closures TO anvilkit_control_app;

-- +goose Down
REVOKE ALL ON effect_observations, obligation_dispositions, recovery_runs, recovery_progress, recovery_findings, admission_closures FROM anvilkit_control_app;
DROP TABLE admission_closures;
DROP TABLE recovery_findings;
DROP TABLE recovery_progress;
DROP TABLE recovery_runs;
DROP TABLE obligation_dispositions;
DROP TABLE effect_observations;
DROP INDEX effect_intents_unresolved_idx;
DROP INDEX effect_intents_command_idx;
ALTER TABLE effect_intents DROP CONSTRAINT effect_intents_state_check;
ALTER TABLE effect_intents ADD CONSTRAINT effect_intents_state_check CHECK (state IN ('prepared', 'sent', 'succeeded', 'failed', 'unknown'));
ALTER TABLE effect_intents
    DROP COLUMN tenant_id, DROP COLUMN attempt_id, DROP COLUMN command_id, DROP COLUMN owner, DROP COLUMN execution_epoch, DROP COLUMN recovery_epoch,
    DROP COLUMN lease_id, DROP COLUMN lease_fence, DROP COLUMN lease_expires_at, DROP COLUMN outcome, DROP COLUMN denial_code, DROP COLUMN deadline, DROP COLUMN observed_at;
