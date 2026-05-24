CREATE SCHEMA IF NOT EXISTS bronze_{schema_prefix};

CREATE TABLE IF NOT EXISTS bronze_{schema_prefix}.accounts (
    account_id    TEXT        NOT NULL,
    customer_id   TEXT        NOT NULL,
    account_type  TEXT,
    balance       NUMERIC(18,2),
    currency      TEXT,
    opened_at     TIMESTAMPTZ,
    status        TEXT,
    closed_at     TIMESTAMPTZ,
    row_hash      TEXT        NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_run_id TEXT        NOT NULL,
    PRIMARY KEY (account_id)
);
