CREATE SCHEMA IF NOT EXISTS silver_{schema_prefix};

CREATE TABLE IF NOT EXISTS silver_{schema_prefix}.customers (
    customer_id      TEXT        NOT NULL,
    full_name        TEXT,
    segment          TEXT,
    onboarded_at     TIMESTAMPTZ,
    country          TEXT,
    email            TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL,
    pipeline_version TEXT        NOT NULL,
    source_system    TEXT        NOT NULL,
    PRIMARY KEY (customer_id)
);
