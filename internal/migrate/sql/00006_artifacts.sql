-- +goose Up
-- P08 (DD-02 §6, contracts.md §2 artifact_transfers / stage_manifests):
-- scoped artifact transfers bind the epochs they were begun under, the
-- Control-owned object key of the bytes (never surfaced to callers or
-- logs), the verified facts of the finalized object and the command that
-- finalized it; an accepted stage binds the epochs it was accepted under
-- and every finalized artifact its result manifest names, with the exact
-- object version verified. Nothing here rewrites an applied migration.

ALTER TABLE artifact_transfers
    ADD COLUMN actor_id            text NOT NULL DEFAULT '',
    ADD COLUMN execution_epoch     bigint NOT NULL DEFAULT 1 CHECK (execution_epoch >= 1),
    ADD COLUMN recovery_epoch      bigint NOT NULL DEFAULT 1 CHECK (recovery_epoch >= 1),
    ADD COLUMN object_key          text NOT NULL DEFAULT '',
    ADD COLUMN actual_size         bigint CHECK (actual_size IS NULL OR actual_size >= 0),
    ADD COLUMN actual_digest       text CHECK (actual_digest IS NULL OR actual_digest ~ '^sha256:[0-9a-f]{64}$'),
    ADD COLUMN finalize_command_id text,
    ADD COLUMN finalize_request_digest text,
    ADD COLUMN updated_at          timestamptz NOT NULL DEFAULT now();
CREATE INDEX artifact_transfers_scope_idx ON artifact_transfers (tenant_id, created_at, transfer_id);
CREATE INDEX artifact_transfers_attempt_idx ON artifact_transfers (attempt_id) WHERE attempt_id IS NOT NULL;

ALTER TABLE stage_manifests
    ADD COLUMN execution_epoch bigint NOT NULL DEFAULT 1 CHECK (execution_epoch >= 1),
    ADD COLUMN recovery_epoch  bigint NOT NULL DEFAULT 1 CHECK (recovery_epoch >= 1);

-- One row per artifact an accepted stage binds: the transfer, the class
-- and digest/size the manifest declared and Control verified, and the exact
-- object version. Immutable with the stage; never garbage-collected here.
CREATE TABLE stage_artifacts (
    stage_id       text NOT NULL REFERENCES stage_manifests (stage_id),
    transfer_id    text NOT NULL REFERENCES artifact_transfers (transfer_id),
    handle         text NOT NULL,
    class          text NOT NULL CHECK (class IN ('prompt', 'brief', 'source', 'stage', 'result', 'evidence', 'answer', 'argument')),
    digest         text NOT NULL CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    size_bytes     bigint NOT NULL CHECK (size_bytes >= 0),
    object_version text NOT NULL,
    PRIMARY KEY (stage_id, transfer_id)
);
CREATE INDEX stage_artifacts_transfer_idx ON stage_artifacts (transfer_id);

GRANT SELECT, INSERT, UPDATE ON stage_artifacts TO anvilkit_control_app;

-- +goose Down
REVOKE ALL ON stage_artifacts FROM anvilkit_control_app;
DROP TABLE stage_artifacts;
ALTER TABLE stage_manifests DROP COLUMN execution_epoch, DROP COLUMN recovery_epoch;
DROP INDEX artifact_transfers_attempt_idx;
DROP INDEX artifact_transfers_scope_idx;
ALTER TABLE artifact_transfers
    DROP COLUMN actor_id, DROP COLUMN execution_epoch, DROP COLUMN recovery_epoch, DROP COLUMN object_key, DROP COLUMN actual_size,
    DROP COLUMN actual_digest, DROP COLUMN finalize_command_id, DROP COLUMN finalize_request_digest, DROP COLUMN updated_at;
