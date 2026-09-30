-- +goose Up
-- P19: a tool-dispatch obligation binds the exact argument digest and
-- whether the method is side-effecting (DD-02 §4, the tool-dispatch class:
-- call/grant revision/server/schema/method/argument digest/exposure). A
-- repeated AdmitTool naming other arguments is an idempotency conflict.
-- Model dispatches and tool dispatches recorded before this migration carry
-- the empty digest.
ALTER TABLE dispatches
    ADD COLUMN argument_digest text NOT NULL DEFAULT '',
    ADD COLUMN side_effecting  boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT dispatches_argument_digest_check CHECK (argument_digest = '' OR argument_digest ~ '^sha256:[0-9a-f]{64}$');

-- +goose Down
ALTER TABLE dispatches
    DROP CONSTRAINT dispatches_argument_digest_check,
    DROP COLUMN side_effecting,
    DROP COLUMN argument_digest;
