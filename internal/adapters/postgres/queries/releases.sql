-- Releases (P21): the committed projection of a release operation,
-- changed only under its expected revision.

-- name: GetRelease :one
SELECT * FROM releases WHERE operation_id = $1;

-- name: LockRelease :one
SELECT * FROM releases WHERE operation_id = $1 FOR UPDATE;

-- name: InsertRelease :exec
INSERT INTO releases (operation_id, tenant_id, lineage, source_revision, state, subject, subject_digest, release_id, review_effect_id,
    approval, approval_deadline, npm, browser, activation, catalog_revision, failure_code, revision, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18);

-- name: UpdateRelease :execrows
UPDATE releases SET state = $2, subject = $3, subject_digest = $4, release_id = $5, review_effect_id = $6, approval = $7,
    approval_deadline = $8, npm = $9, browser = $10, activation = $11, catalog_revision = $12, failure_code = $13, revision = $14, updated_at = $15
WHERE operation_id = $1 AND revision = sqlc.arg(expected_revision);

-- Lineage identities (P0.8): the component identity allocated to a
-- component source lineage, recorded once by the first candidate
-- registration of a Generation on it and never changed.

-- name: InsertLineageIdentity :execrows
INSERT INTO lineage_identities (tenant_id, lineage, component_id, puck_type, package_name, operation_id, brief_id, recorded_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, lineage) DO NOTHING;

-- name: GetLineageIdentity :one
SELECT * FROM lineage_identities WHERE tenant_id = $1 AND lineage = $2;
