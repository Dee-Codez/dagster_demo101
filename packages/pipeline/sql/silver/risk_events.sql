CREATE SCHEMA IF NOT EXISTS silver_{schema_prefix};

CREATE TABLE IF NOT EXISTS silver_{schema_prefix}.risk_events (
    event_id         TEXT        NOT NULL,
    customer_id      TEXT        NOT NULL,
    event_type       TEXT,
    severity         TEXT,
    occurred_at      TIMESTAMPTZ,
    resolved_at      TIMESTAMPTZ,
    notes            TEXT,
    silver_loaded_at TIMESTAMPTZ NOT NULL,
    pipeline_version TEXT        NOT NULL,
    source_system    TEXT        NOT NULL,
    PRIMARY KEY (event_id)
);
