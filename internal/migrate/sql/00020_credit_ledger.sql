-- +goose Up
-- DEC-066 (plan 0007 OD-7, requirement M1-11): the credit ledger, separate
-- from USD spend. A credit pool limits how many operations of one tenant or
-- one actor are funded in one period; funding debits a profile's credits
-- from the current tenant and actor pools in the transaction that allocates
-- its USD amount, and a debited credit is never refunded. Credits are whole
-- numbers: they never enter budget_pools, allocations or cost_entries, so no
-- USD admission, cap or settlement path sees them. Credit pools are rank-1
-- rows like the budget pools (locked after them, in id order); a limit
-- below the used count fences new debits only. The runtime role appends
-- credit entries and never rewrites them (as 00018 does for the ledgers).
-- Additive: a Control release without this migration ignores both tables.

CREATE TABLE credit_pools (
    credit_pool_id text PRIMARY KEY,
    level          text NOT NULL CHECK (level IN ('tenant', 'actor')),
    tenant_id      text NOT NULL,
    actor_id       text,
    period_start   timestamptz NOT NULL,
    period_end     timestamptz NOT NULL,
    limit_credits  bigint NOT NULL CHECK (limit_credits >= 0),
    used_credits   bigint NOT NULL DEFAULT 0 CHECK (used_credits >= 0),
    revision       bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
    created_by     text NOT NULL,
    CHECK (period_end > period_start),
    CHECK ((level = 'tenant') = (actor_id IS NULL))
);
CREATE UNIQUE INDEX credit_pools_scope_idx ON credit_pools (level, tenant_id, coalesce(actor_id, ''), period_start);

CREATE TABLE credit_entries (
    entry_id       text PRIMARY KEY,
    operation_id   text NOT NULL REFERENCES operations (operation_id),
    credit_pool_id text NOT NULL REFERENCES credit_pools (credit_pool_id),
    credits        bigint NOT NULL CHECK (credits > 0),
    created_at     timestamptz NOT NULL,
    UNIQUE (credit_pool_id, operation_id)
);
CREATE INDEX credit_entries_operation_idx ON credit_entries (operation_id);

GRANT SELECT, INSERT, UPDATE ON credit_pools TO anvilkit_control_app;
GRANT SELECT, INSERT ON credit_entries TO anvilkit_control_app;

-- +goose Down
REVOKE ALL ON credit_pools, credit_entries FROM anvilkit_control_app;
DROP TABLE credit_entries;
DROP TABLE credit_pools;
