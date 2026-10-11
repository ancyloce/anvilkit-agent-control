-- +goose Up
-- P1.5: indexes of the admission hot path (CTL-INF-06) and the uniqueness
-- of recovery epochs (CTL-APP-08). The first production install is empty;
-- on a current database the indexes are built with lock_timeout so a long
-- transaction never blocks the migration indefinitely.
SET lock_timeout = '10s';
CREATE INDEX IF NOT EXISTS allocations_operation_pool_idx ON allocations (operation_id, pool_id);
CREATE INDEX IF NOT EXISTS dispatches_operation_state_idx ON dispatches (operation_id, state);
CREATE INDEX IF NOT EXISTS dispatches_attempt_prepared_idx ON dispatches (attempt_id) WHERE state = 'prepared';
CREATE INDEX IF NOT EXISTS cost_entries_dispatch_idx ON cost_entries (dispatch_id);
CREATE INDEX IF NOT EXISTS stage_manifests_operation_accepted_idx ON stage_manifests (operation_id, accepted_at);
CREATE INDEX IF NOT EXISTS permits_owner_granted_idx ON permits (owner_kind, owner_id, granted_at DESC);
-- Recovery epochs are allocated from a sequence and are unique.
CREATE SEQUENCE IF NOT EXISTS recovery_epoch_seq AS bigint START WITH 1;
SELECT setval('recovery_epoch_seq', coalesce((SELECT max(recovery_epoch) FROM recovery_runs), 0) + 1, false);
ALTER TABLE recovery_runs ADD CONSTRAINT recovery_runs_epoch_unique UNIQUE (recovery_epoch);
GRANT USAGE ON SEQUENCE recovery_epoch_seq TO anvilkit_control_app;
RESET lock_timeout;

-- +goose Down
ALTER TABLE recovery_runs DROP CONSTRAINT recovery_runs_epoch_unique;
DROP SEQUENCE IF EXISTS recovery_epoch_seq;
DROP INDEX IF EXISTS permits_owner_granted_idx;
DROP INDEX IF EXISTS stage_manifests_operation_accepted_idx;
DROP INDEX IF EXISTS cost_entries_dispatch_idx;
DROP INDEX IF EXISTS dispatches_attempt_prepared_idx;
DROP INDEX IF EXISTS dispatches_operation_state_idx;
DROP INDEX IF EXISTS allocations_operation_pool_idx;
