CREATE SCHEMA IF NOT EXISTS silver_{schema_prefix};

CREATE TABLE IF NOT EXISTS silver_{schema_prefix}.transactions (
    txn_id           TEXT         NOT NULL,
    account_id       TEXT         NOT NULL,
    amount           NUMERIC(18,2),
    currency         TEXT,
    txn_type         TEXT,
    txn_at           TIMESTAMPTZ,
    description      TEXT,
    status           TEXT,
    silver_loaded_at TIMESTAMPTZ  NOT NULL,
    pipeline_version TEXT         NOT NULL,
    source_system    TEXT         NOT NULL,
    PRIMARY KEY (txn_id)
);
