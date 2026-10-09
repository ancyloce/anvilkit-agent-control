-- +goose Up
-- P0.8 (F-P0.8-1): lineage identities. A frozen brief carries the component
-- identity it allocates (component id, Puck type, package name; briefs
-- frozen before P0.8 have none). lineage_identities records the identity
-- allocated to a component source lineage, once: the first candidate
-- registration of a Generation on the lineage binds its brief's identity in
-- the transaction that prepares the registration; a registration under
-- another identity is denied. Previews and releases of the lineage read it,
-- and a release of a lineage without one is refused.
ALTER TABLE briefs
    ADD COLUMN component_id text,
    ADD COLUMN puck_type    text,
    ADD COLUMN package_name text,
    ADD CONSTRAINT briefs_component_check CHECK ((component_id IS NULL) = (puck_type IS NULL) AND (component_id IS NULL) = (package_name IS NULL));
CREATE TABLE lineage_identities (
    tenant_id    text NOT NULL,
    lineage      text NOT NULL CHECK (lineage ~ '^sha256:[0-9a-f]{64}$'),
    component_id text NOT NULL,
    puck_type    text NOT NULL,
    package_name text NOT NULL,
    operation_id text NOT NULL,
    brief_id     text NOT NULL,
    recorded_at  timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, lineage)
);
GRANT SELECT, INSERT ON lineage_identities TO anvilkit_control_app;

-- +goose Down
DROP TABLE lineage_identities;
ALTER TABLE briefs DROP COLUMN component_id, DROP COLUMN puck_type, DROP COLUMN package_name;
