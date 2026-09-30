-- +goose Up
-- P21: releases (DD-04 §4, DD-06 §4). A release operation names the
-- operation whose saved or registered source it releases and the exact
-- package version; Control binds that source artifact (source_handle).
-- releases is the committed projection the Workflow records under a
-- monotonic revision (compare-and-set) and the API reads: the exact
-- subject (recorded once), the review and the maintainer's decision bound
-- to a digest, each target's effect and verified receipt, the activation.
-- The domain checks every binding before a row changes; the constraints
-- keep the recorded facts consistent with each other.
ALTER TABLE operations ADD COLUMN source_operation_id text, ADD COLUMN package_version text;
CREATE TABLE releases (
    operation_id      text PRIMARY KEY REFERENCES operations (operation_id),
    tenant_id         text NOT NULL,
    lineage           text NOT NULL CHECK (lineage ~ '^sha256:[0-9a-f]{64}$'),
    source_revision   text NOT NULL,
    state             text NOT NULL CHECK (state IN ('certifying', 'awaiting_approval', 'publishing', 'published', 'activated',
                                                     'partially_published', 'reconciling', 'rejected', 'failed')),
    subject           jsonb,
    subject_digest    text CHECK (subject_digest ~ '^sha256:[0-9a-f]{64}$'),
    release_id        text,
    review_effect_id  text,
    approval          jsonb,
    approval_deadline timestamptz,
    npm               jsonb NOT NULL,
    browser           jsonb NOT NULL,
    activation        jsonb NOT NULL,
    catalog_revision  text,
    failure_code      text,
    revision          bigint NOT NULL CHECK (revision > 0),
    updated_at        timestamptz NOT NULL,
    CONSTRAINT releases_subject_check CHECK ((subject IS NULL) = (subject_digest IS NULL)),
    CONSTRAINT releases_review_check CHECK (state IN ('certifying', 'failed') OR (subject IS NOT NULL AND release_id IS NOT NULL)),
    CONSTRAINT releases_activated_check CHECK (state <> 'activated' OR catalog_revision IS NOT NULL),
    CONSTRAINT releases_failed_check CHECK (state NOT IN ('failed', 'rejected') OR failure_code IS NOT NULL)
);
CREATE INDEX releases_tenant_idx ON releases (tenant_id, lineage, updated_at);
CREATE INDEX releases_subject_idx ON releases (tenant_id, subject_digest) WHERE subject_digest IS NOT NULL;
GRANT SELECT, INSERT, UPDATE ON releases TO anvilkit_control_app;

-- +goose Down
DROP TABLE releases;
ALTER TABLE operations DROP COLUMN source_operation_id, DROP COLUMN package_version;
