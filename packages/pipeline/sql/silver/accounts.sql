CREATE SCHEMA IF NOT EXISTS silver_{schema_prefix};

CREATE TABLE IF NOT EXISTS silver_{schema_prefix}.accounts (
    account_id       TEXT         NOT NULL,
    customer_id      TEXT         NOT NULL,
    account_type     TEXT,
    balance          NUMERIC(18,2),
    currency         TEXT,
    opened_at        TIMESTAMPTZ,
    status           TEXT,
    closed_at        TIMESTAMPTZ,
    silver_loaded_at TIMESTAMPTZ  NOT NULL,
    pipeline_version TEXT         NOT NULL,
    source_system    TEXT         NOT NULL,
    PRIMARY KEY (account_id)
);
