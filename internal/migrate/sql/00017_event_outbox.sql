-- +goose Up
-- P2.8: the transactional outbox of Control's domain events (DD-09 §2,
-- contracts.md §4 "result/view/event acceptance"). outbox / outbox_offsets
-- are the physical messages/offsets schema of the pinned watermill-sql
-- v4.1.5 PostgreSQL adapter (DefaultPostgreSQLSchema /
-- DefaultPostgreSQLOffsetsAdapter with the table names generated as
-- "outbox" and "outbox_offsets"), identical to Knowledge's and MCP's:
-- offset BIGSERIAL, uuid VARCHAR(36) (the eventId; business identifiers
-- travel in the envelope), payload and metadata JSON, transaction_id xid8
-- from pg_current_xact_id(), ordered by (transaction_id, offset) under the
-- pg_snapshot_xmin visibility watermark. Every operation event row
-- (operation_events) inserts its operation.changed envelope here on the
-- same transaction (lock rank 7); Control's in-process forwarder module
-- reads the messages, owns the subscriber offsets and publishes to
-- JetStream. Runtime schema initialization is disabled.
CREATE TABLE outbox (
    "offset"         BIGSERIAL,
    "uuid"           VARCHAR(36) NOT NULL,
    "created_at"     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "payload"        JSON DEFAULT NULL,
    "metadata"       JSON DEFAULT NULL,
    "transaction_id" xid8 NOT NULL,
    PRIMARY KEY ("transaction_id", "offset")
);

CREATE TABLE outbox_offsets (
    consumer_group                VARCHAR(255) NOT NULL,
    offset_acked                  BIGINT,
    last_processed_transaction_id xid8 NOT NULL,
    PRIMARY KEY (consumer_group)
);

-- The retention sweep of forwarded rows reads by age.
CREATE INDEX outbox_created_at_idx ON outbox (created_at);

GRANT SELECT, INSERT ON outbox TO anvilkit_control_app;
GRANT USAGE ON SEQUENCE outbox_offset_seq TO anvilkit_control_app;
-- The forwarder is a module of the service (DD-09 §2 "Control/MCP Go relay
-- can be service modules"): the app role owns the subscriber offsets and
-- deletes the rows its consumer group acknowledged past the retention.
GRANT SELECT, INSERT, UPDATE ON outbox_offsets TO anvilkit_control_app;
GRANT DELETE ON outbox TO anvilkit_control_app;

-- +goose Down
REVOKE ALL ON outbox, outbox_offsets FROM anvilkit_control_app;
REVOKE ALL ON SEQUENCE outbox_offset_seq FROM anvilkit_control_app;
DROP TABLE outbox_offsets;
DROP TABLE outbox;
