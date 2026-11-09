-- RB-A27: the retention identity of operation events
-- (anvilkit_control_retention). The app role appends events and never
-- deletes them (00018); this role reads the operations it judges and
-- deletes their events, and writes nothing else. The role is created by the
-- placement beside the app and migrator roles; this migration grants it.
-- The partial index serves the retention's selection: settled terminal
-- operations by their last change.

-- +goose Up
GRANT USAGE ON SCHEMA public TO anvilkit_control_retention;
GRANT SELECT ON operations TO anvilkit_control_retention;
GRANT SELECT, DELETE ON operation_events TO anvilkit_control_retention;
CREATE INDEX operations_retention_idx ON operations (updated_at)
    WHERE lifecycle IN ('succeeded', 'failed', 'canceled') AND finance_state = 'settled';

-- +goose Down
DROP INDEX operations_retention_idx;
REVOKE ALL ON operations, operation_events FROM anvilkit_control_retention;
REVOKE USAGE ON SCHEMA public FROM anvilkit_control_retention;
