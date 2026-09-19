-- +goose Up
-- Immutable business outcome retained while finance/effect cleanup reconciles.
CREATE TABLE operation_settlements (
    operation_id text PRIMARY KEY REFERENCES operations (operation_id),
    command_id text NOT NULL,
    request_digest text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'canceled')),
    failure_code text NOT NULL,
    phase text NOT NULL
);
GRANT SELECT, INSERT ON operation_settlements TO anvilkit_control_app;

-- +goose Down
DROP TABLE operation_settlements;
