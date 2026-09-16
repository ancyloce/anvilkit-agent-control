-- +goose Up
-- anvilkit_control: Control is the sole writer (contracts.md §1–§4). Runs as
-- anvilkit_control_migrator; the runtime role anvilkit_control_app receives DML
-- only. Cross-domain references are immutable identifiers, never foreign keys.
-- Revisions, sequences, epochs and counters are bigint here and canonical
-- decimal strings on every wire. Money is an integer quantity at scale 6.
-- The Watermill outbox physical schema is created by its pinned adapter's
-- qualified migration in P14; operation_events is the durable SSE source.

CREATE TABLE operations (
    operation_id        text PRIMARY KEY,
    tenant_id           text NOT NULL,
    project_id          text NOT NULL DEFAULT '',
    actor_id            text NOT NULL,
    command_id          text NOT NULL,
    kind                text NOT NULL CHECK (kind IN ('preparation', 'generation', 'refinement', 'preview_build', 'release', 'local_check')),
    profile_id          text NOT NULL,
    subject_digest      text NOT NULL CHECK (subject_digest ~ '^sha256:[0-9a-f]{64}$'),
    brief_id            text,
    source_revision     text,
    semantic_digest     text NOT NULL CHECK (semantic_digest ~ '^sha256:[0-9a-f]{64}$'),
    lifecycle           text NOT NULL CHECK (lifecycle IN ('accepted', 'running', 'waiting', 'reconciling', 'suspended', 'succeeded', 'failed', 'canceled')),
    phase               text NOT NULL,
    control_state       text NOT NULL CHECK (control_state IN ('none', 'cancel_pending', 'cancel_applied', 'hold_pending', 'hold_applied')),
    cleanup_state       text NOT NULL CHECK (cleanup_state IN ('not_required', 'pending', 'complete', 'unknown')),
    finance_state       text NOT NULL CHECK (finance_state IN ('not_funded', 'funded', 'settling', 'settled', 'exposure_unknown')),
    failure_code        text,
    revision            bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
    next_event_seq      bigint NOT NULL DEFAULT 1 CHECK (next_event_seq >= 1),
    execution_epoch     bigint NOT NULL DEFAULT 1 CHECK (execution_epoch >= 1),
    recovery_epoch      bigint NOT NULL DEFAULT 1 CHECK (recovery_epoch >= 1),
    deadline            timestamptz NOT NULL,
    intake_state        text NOT NULL CHECK (intake_state IN ('pending', 'confirmed')),
    intake_version      text,
    relay_state         text NOT NULL CHECK (relay_state IN ('pending', 'started', 'cancel_pending', 'settled')),
    relay_run_id        text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, command_id)
);
CREATE INDEX operations_tenant_created_idx ON operations (tenant_id, created_at, operation_id);
CREATE INDEX operations_active_deadline_idx ON operations (deadline) WHERE lifecycle IN ('accepted', 'running', 'waiting', 'reconciling', 'suspended');
CREATE INDEX operations_intake_pending_idx ON operations (created_at) WHERE intake_state = 'pending';
CREATE INDEX operations_relay_pending_idx ON operations (created_at) WHERE relay_state IN ('pending', 'cancel_pending');

CREATE TABLE operation_commands (
    tenant_id                    text NOT NULL,
    command_id                   text NOT NULL,
    operation_id                 text NOT NULL REFERENCES operations (operation_id),
    actor_id                     text NOT NULL,
    kind                         text NOT NULL CHECK (kind IN ('cancel', 'hold', 'resume', 'change_definition')),
    expected_revision            bigint NOT NULL,
    request_digest               text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    target_definition_activation text,
    outcome                      text NOT NULL CHECK (outcome IN ('pending', 'applied', 'blocked', 'rejected')),
    reason_code                  text,
    operation_revision           bigint NOT NULL,
    accepted_at                  timestamptz NOT NULL DEFAULT now(),
    settled_at                   timestamptz,
    PRIMARY KEY (tenant_id, command_id)
);
CREATE INDEX operation_commands_operation_idx ON operation_commands (operation_id, accepted_at);

CREATE TABLE operation_events (
    operation_id  text NOT NULL REFERENCES operations (operation_id),
    event_seq     bigint NOT NULL CHECK (event_seq >= 1),
    transition_id text NOT NULL,
    event_type    text NOT NULL,
    revision      bigint NOT NULL,
    payload       jsonb NOT NULL,
    occurred_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (operation_id, event_seq),
    UNIQUE (operation_id, transition_id)
);

CREATE TABLE attempts (
    attempt_id        text PRIMARY KEY,
    operation_id      text NOT NULL REFERENCES operations (operation_id),
    tenant_id         text NOT NULL,
    step_id           text NOT NULL,
    visit_ordinal     bigint NOT NULL CHECK (visit_ordinal >= 0),
    attempt_ordinal   bigint NOT NULL CHECK (attempt_ordinal >= 1),
    profile_id        text NOT NULL,
    execution_epoch   bigint NOT NULL,
    command_id        text NOT NULL,
    request_digest    text NOT NULL,
    state             text NOT NULL CHECK (state IN ('open', 'launch_prepared', 'running', 'result_accepted', 'closed')),
    outcome           text CHECK (outcome IN ('completed', 'failed', 'infrastructure_failed', 'canceled', 'unknown')),
    cleanup_state     text CHECK (cleanup_state IN ('not_required', 'pending', 'complete', 'unknown')),
    failure_code      text,
    accepted_stage_id text,
    deadline          timestamptz NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (operation_id, step_id, visit_ordinal, attempt_ordinal),
    UNIQUE (operation_id, command_id)
);

-- job-launch obligation: persisted (and inventoried) before any Kubernetes create.
CREATE TABLE launches (
    launch_id        text PRIMARY KEY,
    attempt_id       text NOT NULL REFERENCES attempts (attempt_id),
    operation_id     text NOT NULL REFERENCES operations (operation_id),
    launch_key       text NOT NULL,
    backend          text NOT NULL,
    profile_id       text NOT NULL,
    image_digest     text NOT NULL CHECK (image_digest ~ '^sha256:[0-9a-f]{64}$'),
    execution_epoch  bigint NOT NULL,
    launch_epoch     bigint NOT NULL,
    deadline         timestamptz NOT NULL,
    command_id       text NOT NULL,
    request_digest   text NOT NULL,
    inventory_state  text NOT NULL CHECK (inventory_state IN ('pending', 'confirmed', 'uncertain')),
    inventory_version text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (backend, launch_key),
    UNIQUE (attempt_id, command_id)
);

CREATE TABLE physical_instances (
    instance_id   text PRIMARY KEY,
    attempt_id    text NOT NULL REFERENCES attempts (attempt_id),
    launch_id     text NOT NULL REFERENCES launches (launch_id),
    launch_key    text NOT NULL,
    backend       text NOT NULL,
    job_uid       text NOT NULL,
    pod_uid       text NOT NULL,
    image_digest  text NOT NULL,
    launch_epoch  bigint NOT NULL,
    phase         text NOT NULL CHECK (phase IN ('pending', 'running', 'succeeded', 'failed', 'unknown')),
    exit_code     integer,
    is_current    boolean NOT NULL DEFAULT false,
    registered_at timestamptz NOT NULL DEFAULT now(),
    observed_at   timestamptz,
    UNIQUE (backend, pod_uid)
);
-- Exactly one current physical owner per attempt; duplicate Pods are registered but not current.
CREATE UNIQUE INDEX physical_instances_current_idx ON physical_instances (attempt_id) WHERE is_current;

CREATE TABLE stage_manifests (
    stage_id          text PRIMARY KEY,
    attempt_id        text NOT NULL REFERENCES attempts (attempt_id),
    instance_id       text NOT NULL REFERENCES physical_instances (instance_id),
    operation_id      text NOT NULL REFERENCES operations (operation_id),
    phase_ordinal     bigint NOT NULL CHECK (phase_ordinal >= 1),
    profile_id        text NOT NULL,
    verdict           text NOT NULL CHECK (verdict IN ('certified', 'repairable', 'invalid', 'infrastructure_failed', 'canceled')),
    failure_code      text,
    result_digest     text NOT NULL CHECK (result_digest ~ '^sha256:[0-9a-f]{64}$'),
    result_manifest   jsonb NOT NULL,
    observer_identity text NOT NULL,
    command_id        text NOT NULL,
    request_digest    text NOT NULL,
    accepted_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (attempt_id, phase_ordinal)
);

-- Budgets, admission and costs (DD-02 §3–§4). Amounts are scale-6 integers.
CREATE TABLE budget_pools (
    pool_id       text PRIMARY KEY,
    level         text NOT NULL CHECK (level IN ('platform', 'tenant', 'actor')),
    tenant_id     text,
    actor_id      text,
    period_start  timestamptz NOT NULL,
    period_end    timestamptz NOT NULL,
    currency      text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    cap_amount    bigint NOT NULL CHECK (cap_amount >= 0),
    allocated     bigint NOT NULL DEFAULT 0 CHECK (allocated >= 0),
    revision      bigint NOT NULL DEFAULT 1,
    CHECK (period_end > period_start)
);
CREATE UNIQUE INDEX budget_pools_scope_idx ON budget_pools (level, coalesce(tenant_id, ''), coalesce(actor_id, ''), period_start);

CREATE TABLE allocations (
    allocation_id text PRIMARY KEY,
    pool_id       text NOT NULL REFERENCES budget_pools (pool_id),
    operation_id  text NOT NULL REFERENCES operations (operation_id),
    currency      text NOT NULL,
    amount        bigint NOT NULL CHECK (amount >= 0),
    reserved      bigint NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    consumed      bigint NOT NULL DEFAULT 0 CHECK (consumed >= 0),
    revision      bigint NOT NULL DEFAULT 1,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (pool_id, operation_id)
);

CREATE TABLE dispatches (
    dispatch_id       text PRIMARY KEY,
    kind              text NOT NULL CHECK (kind IN ('model', 'tool')),
    tenant_id         text NOT NULL,
    operation_id      text NOT NULL REFERENCES operations (operation_id),
    attempt_id        text NOT NULL REFERENCES attempts (attempt_id),
    instance_id       text,
    call_id           text NOT NULL,
    owner             text NOT NULL,
    request_digest    text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    route_id          text,
    grant_id          text,
    grant_revision    bigint,
    execution_epoch   bigint NOT NULL,
    state             text NOT NULL CHECK (state IN ('prepared', 'authorized', 'observed', 'unknown', 'confirmed_not_sent', 'denied')),
    outcome           text CHECK (outcome IN ('succeeded', 'failed', 'canceled', 'unknown')),
    reserved_currency text NOT NULL,
    reserved_amount   bigint NOT NULL CHECK (reserved_amount >= 0),
    meter_revision    text NOT NULL DEFAULT '',
    supersedes_call_id text,
    evidence_ref      text,
    inventory_state   text NOT NULL CHECK (inventory_state IN ('pending', 'confirmed', 'uncertain')),
    inventory_version text,
    deadline          timestamptz NOT NULL,
    admitted_at       timestamptz NOT NULL DEFAULT now(),
    observed_at       timestamptz,
    UNIQUE (tenant_id, owner, call_id)
);
CREATE INDEX dispatches_unresolved_idx ON dispatches (admitted_at) WHERE state IN ('prepared', 'authorized', 'unknown');

CREATE TABLE usage_observations (
    observation_id      text PRIMARY KEY,
    dispatch_id         text NOT NULL REFERENCES dispatches (dispatch_id),
    source              text NOT NULL,
    sequence            bigint NOT NULL CHECK (sequence >= 0),
    native_reference    text NOT NULL DEFAULT '',
    input_units         bigint NOT NULL CHECK (input_units >= 0),
    output_units        bigint NOT NULL CHECK (output_units >= 0),
    reasoning_units     bigint NOT NULL CHECK (reasoning_units >= 0),
    cached_input_units  bigint NOT NULL CHECK (cached_input_units >= 0),
    cost_revision       text NOT NULL DEFAULT '',
    observed_at         timestamptz NOT NULL,
    UNIQUE (dispatch_id, source, sequence)
);

CREATE TABLE cost_entries (
    entry_id       text PRIMARY KEY,
    operation_id   text NOT NULL REFERENCES operations (operation_id),
    dispatch_id    text REFERENCES dispatches (dispatch_id),
    observation_id text REFERENCES usage_observations (observation_id),
    kind           text NOT NULL CHECK (kind IN ('estimate', 'actual', 'correction', 'credit')),
    currency       text NOT NULL,
    amount         bigint NOT NULL,
    correction_of  text REFERENCES cost_entries (entry_id),
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX cost_entries_observation_kind_idx ON cost_entries (observation_id, kind) WHERE observation_id IS NOT NULL;
CREATE INDEX cost_entries_operation_idx ON cost_entries (operation_id, created_at);

CREATE TABLE resource_pools (
    pool_id  text PRIMARY KEY,
    class    text NOT NULL,
    capacity integer NOT NULL CHECK (capacity >= 0),
    reserved_control integer NOT NULL DEFAULT 0 CHECK (reserved_control >= 0)
);

CREATE TABLE permits (
    permit_id     text PRIMARY KEY,
    pool_id       text NOT NULL REFERENCES resource_pools (pool_id),
    owner_kind    text NOT NULL CHECK (owner_kind IN ('operation', 'attempt', 'instance')),
    owner_id      text NOT NULL,
    fence_epoch   bigint NOT NULL,
    state         text NOT NULL CHECK (state IN ('active', 'released', 'expired_unconfirmed')),
    granted_at    timestamptz NOT NULL DEFAULT now(),
    released_at   timestamptz,
    release_evidence text
);
CREATE UNIQUE INDEX permits_active_owner_idx ON permits (pool_id, owner_kind, owner_id) WHERE state = 'active';

CREATE TABLE effect_intents (
    effect_id         text PRIMARY KEY,
    operation_id      text NOT NULL REFERENCES operations (operation_id),
    kind              text NOT NULL CHECK (kind IN ('business_write', 'publication', 'activation', 'review')),
    occurrence        bigint NOT NULL CHECK (occurrence >= 1),
    canonical_subject text NOT NULL,
    request_digest    text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    expected_revision text,
    state             text NOT NULL CHECK (state IN ('prepared', 'sent', 'succeeded', 'failed', 'unknown')),
    outcome_ref       text,
    query_ref         text,
    inventory_state   text NOT NULL CHECK (inventory_state IN ('pending', 'confirmed', 'uncertain')),
    inventory_version text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (operation_id, kind, occurrence)
);

CREATE TABLE artifact_transfers (
    transfer_id     text PRIMARY KEY,
    tenant_id       text NOT NULL,
    operation_id    text,
    attempt_id      text,
    class           text NOT NULL CHECK (class IN ('prompt', 'brief', 'source', 'stage', 'result', 'evidence', 'answer', 'argument')),
    media_type      text NOT NULL,
    expected_digest text NOT NULL CHECK (expected_digest ~ '^sha256:[0-9a-f]{64}$'),
    expected_size   bigint NOT NULL CHECK (expected_size >= 0),
    handle          text NOT NULL UNIQUE,
    state           text NOT NULL CHECK (state IN ('begun', 'finalized', 'rejected', 'expired')),
    object_version  text,
    reason_code     text,
    command_id      text NOT NULL,
    request_digest  text NOT NULL,
    deadline        timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    finalized_at    timestamptz,
    UNIQUE (tenant_id, command_id)
);

-- MCP grant policy projection and revocation barrier (DD-08 §2); MCP owns the grant.
CREATE TABLE grant_policies (
    grant_id        text NOT NULL,
    grant_revision  bigint NOT NULL,
    tenant_id       text NOT NULL,
    policy_digest   text NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    server_id       text NOT NULL,
    methods         text[] NOT NULL,
    cost_cap_currency text,
    cost_cap_amount bigint,
    expires_at      timestamptz,
    policy_epoch    bigint NOT NULL,
    receipt_id      text NOT NULL UNIQUE,
    revocation_state text NOT NULL CHECK (revocation_state IN ('none', 'fenced', 'converging', 'converged')),
    fenced_at       timestamptz,
    converged_at    timestamptz,
    registered_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (grant_id, grant_revision)
);

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO anvilkit_control_app;
GRANT USAGE ON SCHEMA public TO anvilkit_control_app;

-- +goose Down
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM anvilkit_control_app;
DROP TABLE grant_policies;
DROP TABLE artifact_transfers;
DROP TABLE effect_intents;
DROP TABLE permits;
DROP TABLE resource_pools;
DROP TABLE cost_entries;
DROP TABLE usage_observations;
DROP TABLE dispatches;
DROP TABLE allocations;
DROP TABLE budget_pools;
DROP TABLE stage_manifests;
DROP TABLE physical_instances;
DROP TABLE launches;
DROP TABLE attempts;
DROP TABLE operation_events;
DROP TABLE operation_commands;
DROP TABLE operations;
