-- P3.7 (plan 0004): the runtime role appends to its ledgers and never
-- rewrites them, and never writes migration bookkeeping. 00001 granted
-- SELECT, INSERT, UPDATE on every table (goose_db_version included, which
-- goose creates before 00001 runs); the ledgers keep SELECT and INSERT, the
-- version table SELECT only (the replica's schema check reads it). A
-- correction of a ledger is a new row (cost_entries.kind = 'correction'),
-- never an update.

-- +goose Up
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON goose_db_version FROM anvilkit_control_app;
REVOKE UPDATE, DELETE, TRUNCATE ON cost_entries, usage_observations, operation_events FROM anvilkit_control_app;

-- +goose Down
GRANT INSERT, UPDATE ON goose_db_version TO anvilkit_control_app;
GRANT UPDATE ON cost_entries, usage_observations, operation_events TO anvilkit_control_app;
