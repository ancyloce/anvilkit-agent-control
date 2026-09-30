-- +goose Up
-- P18: the grant policy registration and revocation barrier (DD-08 §2,
-- DD-02 §4). MCP owns the grant; Control owns this projection, its receipt
-- and the barrier. A registration is idempotent by the grant revision and
-- its command (a retried or unanswered RegisterPolicy returns the same
-- receipt); a different policy digest for a registered revision is a
-- conflict, never a replacement. The policy epoch is assigned by Control.
-- A revocation is idempotent by its command: the first BeginRevocation
-- fences new admission in its transaction (tool admission share-locks the
-- policy row while it consumes a permission, so the fence waits for any
-- admission already deciding and every later one sees it); in_flight_calls
-- (permission consumed, outcome not yet observed) and unknown_calls are the
-- counts of the last GetRevocation, converged only when both reach zero.
-- A revocation of a revision Control never registered records a tombstone
-- (registered = false, converged at once): a registration request still in
-- flight from before the revocation can then never make it executable.
CREATE SEQUENCE grant_policy_epoch_seq;
ALTER TABLE grant_policies
    ADD COLUMN register_command_id       text NOT NULL DEFAULT '',
    ADD COLUMN register_request_digest   text NOT NULL DEFAULT '',
    ADD COLUMN revocation_command_id     text,
    ADD COLUMN revocation_request_digest text,
    ADD COLUMN in_flight_calls           bigint NOT NULL DEFAULT 0 CHECK (in_flight_calls >= 0),
    ADD COLUMN unknown_calls             bigint NOT NULL DEFAULT 0 CHECK (unknown_calls >= 0),
    ADD COLUMN registered                boolean NOT NULL DEFAULT true;
-- Policies registered before this barrier carry a deterministic identity of
-- their revision; a revocation already begun carries one of its grant.
UPDATE grant_policies SET register_command_id = 'pre-barrier:' || grant_id || ':' || grant_revision,
                          register_request_digest = policy_digest;
UPDATE grant_policies SET revocation_command_id = 'pre-barrier-revoke:' || grant_id || ':' || grant_revision,
                          revocation_request_digest = policy_digest,
                          fenced_at = coalesce(fenced_at, now()),
                          converged_at = CASE WHEN revocation_state = 'converged' THEN coalesce(converged_at, now()) ELSE converged_at END
    WHERE revocation_state <> 'none';
ALTER TABLE grant_policies
    ALTER COLUMN register_command_id DROP DEFAULT,
    ALTER COLUMN register_request_digest DROP DEFAULT,
    ADD CONSTRAINT grant_policies_register_command UNIQUE (tenant_id, register_command_id),
    ADD CONSTRAINT grant_policies_revocation_command UNIQUE (tenant_id, revocation_command_id),
    ADD CONSTRAINT grant_policies_fenced_check CHECK (revocation_state = 'none' OR (fenced_at IS NOT NULL AND revocation_command_id IS NOT NULL)),
    ADD CONSTRAINT grant_policies_tombstone_check CHECK (registered OR revocation_state = 'converged'),
    ADD CONSTRAINT grant_policies_converged_check CHECK (revocation_state <> 'converged' OR (converged_at IS NOT NULL AND in_flight_calls = 0 AND unknown_calls = 0));
CREATE INDEX dispatches_grant_open_idx ON dispatches (grant_id, grant_revision) WHERE grant_id IS NOT NULL AND state IN ('authorized', 'unknown');
GRANT USAGE ON SEQUENCE grant_policy_epoch_seq TO anvilkit_control_app;

-- +goose Down
DROP INDEX dispatches_grant_open_idx;
ALTER TABLE grant_policies
    DROP CONSTRAINT grant_policies_converged_check,
    DROP CONSTRAINT grant_policies_tombstone_check,
    DROP CONSTRAINT grant_policies_fenced_check,
    DROP CONSTRAINT grant_policies_revocation_command,
    DROP CONSTRAINT grant_policies_register_command,
    DROP COLUMN registered,
    DROP COLUMN unknown_calls,
    DROP COLUMN in_flight_calls,
    DROP COLUMN revocation_request_digest,
    DROP COLUMN revocation_command_id,
    DROP COLUMN register_request_digest,
    DROP COLUMN register_command_id;
DROP SEQUENCE grant_policy_epoch_seq;
