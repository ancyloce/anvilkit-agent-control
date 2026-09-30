-- Preview builds (P20): the committed projection of a preview_build
-- operation, changed only under its expected revision.

-- name: GetPreview :one
SELECT * FROM previews WHERE operation_id = $1;

-- name: LockPreview :one
SELECT * FROM previews WHERE operation_id = $1 FOR UPDATE;

-- name: InsertPreview :exec
INSERT INTO previews (operation_id, tenant_id, subject_digest, base_revision, state, source_revision, current_revision, source_digest,
    module, styles, build_profile_id, host_profile_id, failure_code, revision, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);

-- name: UpdatePreview :execrows
UPDATE previews SET state = $2, source_revision = $3, current_revision = $4, source_digest = $5, module = $6, styles = $7,
    build_profile_id = $8, host_profile_id = $9, failure_code = $10, revision = $11, updated_at = $12
WHERE operation_id = $1 AND revision = sqlc.arg(expected_revision);

-- name: GetCertifiedSourceArtifact :one
SELECT sa.* FROM stage_artifacts sa JOIN stage_manifests sm ON sm.stage_id = sa.stage_id
WHERE sm.operation_id = $1 AND sa.class = 'source' AND sm.verdict = 'certified'
ORDER BY sm.accepted_at DESC LIMIT 1;
