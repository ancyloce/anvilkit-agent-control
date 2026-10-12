-- +goose Up
-- P2.1: the durable price book, the tenant/project/operation actual-cost
-- caps and the return of unspent allocations at financial settlement
-- (DD-02 §3, ENV-06). Price revisions are immutable rows: withdrawing one
-- stops new admission under it, while dispatches prepared under it keep
-- metering under the same revision until they settle. Cost caps are rank-1
-- rows like the pools; their counters hold the reserved exposure and the
-- consumed cost of their scope (an operation cap is checked against the
-- operation's own allocation and keeps no counters).

CREATE TABLE price_revisions (
    revision                  text PRIMARY KEY,
    kind                      text NOT NULL CHECK (kind IN ('model', 'tool')),
    route                     text NOT NULL,
    provider                  text NOT NULL DEFAULT '',
    model                     text NOT NULL DEFAULT '',
    currency                  text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    effective_from            timestamptz NOT NULL,
    effective_until           timestamptz NOT NULL,
    input_units               bigint NOT NULL CHECK (input_units >= 0),
    output_units              bigint NOT NULL CHECK (output_units >= 0),
    reasoning_units           bigint NOT NULL CHECK (reasoning_units >= 0),
    cached_input_units        bigint NOT NULL CHECK (cached_input_units >= 0),
    max_exposure              bigint NOT NULL CHECK (max_exposure > 0),
    input_includes_cached     boolean NOT NULL,
    output_includes_reasoning boolean NOT NULL,
    digest                    text NOT NULL CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    published_by              text NOT NULL,
    command_id                text NOT NULL,
    published_at              timestamptz NOT NULL,
    withdrawn_at              timestamptz,
    withdrawn_by              text,
    CHECK (effective_until > effective_from),
    CHECK ((kind = 'model' AND provider <> '' AND model <> '') OR (kind = 'tool' AND provider = '' AND model = ''))
);
CREATE INDEX price_revisions_route_idx ON price_revisions (kind, route, effective_from DESC) WHERE withdrawn_at IS NULL;

CREATE TABLE cost_caps (
    cap_id       text PRIMARY KEY,
    level        text NOT NULL CHECK (level IN ('tenant', 'project', 'operation')),
    tenant_id    text NOT NULL,
    project_id   text NOT NULL DEFAULT '',
    currency     text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    cap_amount   bigint NOT NULL CHECK (cap_amount >= 0),
    reserved     bigint NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    consumed     bigint NOT NULL DEFAULT 0 CHECK (consumed >= 0),
    revision     bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
    updated_by   text NOT NULL,
    updated_at   timestamptz NOT NULL,
    CHECK (level <> 'tenant' OR project_id = ''),
    UNIQUE (level, tenant_id, project_id, currency)
);

-- The unspent part of a terminal operation's allocation, returned to its
-- pool once (the return ledger; allocations keep their amount).
CREATE TABLE allocation_returns (
    allocation_id text PRIMARY KEY REFERENCES allocations (allocation_id),
    pool_id       text NOT NULL REFERENCES budget_pools (pool_id),
    operation_id  text NOT NULL REFERENCES operations (operation_id),
    currency      text NOT NULL,
    amount        bigint NOT NULL CHECK (amount >= 0),
    returned_at   timestamptz NOT NULL
);
CREATE INDEX allocation_returns_pool_idx ON allocation_returns (pool_id);

-- The financial settlement sweep: terminal operations whose finance has
-- not settled yet.
CREATE INDEX operations_finance_unsettled_idx ON operations (updated_at)
    WHERE finance_state IN ('not_funded', 'funded') AND lifecycle IN ('succeeded', 'failed', 'canceled');

-- Pools are provisioned by the operator RPCs (P2.1) instead of seeded rows.
ALTER TABLE budget_pools ADD COLUMN created_by text NOT NULL DEFAULT '';

GRANT SELECT, INSERT, UPDATE ON price_revisions, cost_caps TO anvilkit_control_app;
GRANT SELECT, INSERT ON allocation_returns TO anvilkit_control_app;
GRANT SELECT, INSERT, UPDATE ON budget_pools TO anvilkit_control_app;

-- +goose Down
REVOKE ALL ON price_revisions, cost_caps, allocation_returns FROM anvilkit_control_app;
ALTER TABLE budget_pools DROP COLUMN created_by;
DROP INDEX operations_finance_unsettled_idx;
DROP TABLE allocation_returns;
DROP TABLE cost_caps;
DROP TABLE price_revisions;
