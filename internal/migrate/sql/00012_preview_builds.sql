-- +goose Up
-- P20: preview builds (DD-04 §5, DD-05 §1). A preview_build operation's
-- subject names the finalized source artifact holding the edited source
-- (source_handle; its digest is the subject digest) and the source revision
-- the edit was based on. previews is the committed projection the Workflow
-- records under a monotonic revision (compare-and-set) and the API reads:
-- the conditional save's outcome (the saved revision, or the current one on
-- a conflict), then the build's exact module and stylesheets. A preview
-- superseded by a newer saved revision is stale: readable, never current.
ALTER TABLE operations ADD COLUMN source_handle text;
CREATE TABLE previews (
    operation_id     text PRIMARY KEY REFERENCES operations (operation_id),
    tenant_id        text NOT NULL,
    subject_digest   text NOT NULL CHECK (subject_digest ~ '^sha256:[0-9a-f]{64}$'),
    base_revision    text NOT NULL,
    state            text NOT NULL CHECK (state IN ('saving', 'conflict', 'building', 'ready', 'stale', 'failed')),
    source_revision  text,
    current_revision text,
    source_digest    text NOT NULL CHECK (source_digest ~ '^sha256:[0-9a-f]{64}$'),
    module           jsonb,
    styles           jsonb NOT NULL DEFAULT '[]'::jsonb,
    build_profile_id text NOT NULL,
    host_profile_id  text NOT NULL,
    failure_code     text,
    revision         bigint NOT NULL CHECK (revision > 0),
    updated_at       timestamptz NOT NULL,
    CONSTRAINT previews_saved_check CHECK (state NOT IN ('building', 'ready', 'stale') OR source_revision IS NOT NULL),
    CONSTRAINT previews_conflict_check CHECK (state <> 'conflict' OR current_revision IS NOT NULL),
    CONSTRAINT previews_built_check CHECK (state NOT IN ('ready', 'stale') OR module IS NOT NULL),
    CONSTRAINT previews_failed_check CHECK (state <> 'failed' OR failure_code IS NOT NULL)
);
CREATE INDEX previews_tenant_idx ON previews (tenant_id, subject_digest, updated_at);
GRANT SELECT, INSERT, UPDATE ON previews TO anvilkit_control_app;

-- +goose Down
DROP TABLE previews;
ALTER TABLE operations DROP COLUMN source_handle;
