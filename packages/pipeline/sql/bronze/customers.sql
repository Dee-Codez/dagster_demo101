CREATE SCHEMA IF NOT EXISTS bronze_{schema_prefix};

CREATE TABLE IF NOT EXISTS bronze_{schema_prefix}.customers (
    customer_id   TEXT        NOT NULL,
    full_name     TEXT,
    segment       TEXT,
    onboarded_at  TIMESTAMPTZ,
    country       TEXT,
    email         TEXT,
    row_hash      TEXT        NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_run_id TEXT        NOT NULL,
    PRIMARY KEY (customer_id)
);
