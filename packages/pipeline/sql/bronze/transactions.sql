CREATE SCHEMA IF NOT EXISTS bronze_{schema_prefix};

CREATE TABLE IF NOT EXISTS bronze_{schema_prefix}.transactions (
    txn_id        TEXT        NOT NULL,
    account_id    TEXT        NOT NULL,
    amount        NUMERIC(18,2),
    currency      TEXT,
    txn_type      TEXT,
    txn_at        TIMESTAMPTZ,
    description   TEXT,
    status        TEXT,
    row_hash      TEXT        NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_run_id TEXT        NOT NULL,
    PRIMARY KEY (txn_id)
);
