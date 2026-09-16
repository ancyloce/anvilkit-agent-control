-- +goose Up
-- Accepted results keep their original bytes (contracts.md §2 "immutable
-- accepted stage", DD-02 §6). result_manifest remains the schema-constrained
-- jsonb copy used for querying; jsonb normalizes member order and whitespace,
-- so it cannot reproduce result_digest. result_manifest_bytes holds the exact
-- bytes the trusted observer submitted, which hash to result_digest. Rows
-- accepted before this migration have no original bytes: they are left NULL
-- and are never backfilled from the normalized copy, so a stored copy is
-- never presented as the original.
ALTER TABLE stage_manifests ADD COLUMN result_manifest_bytes bytea;

-- +goose Down
ALTER TABLE stage_manifests DROP COLUMN result_manifest_bytes;
