CREATE SCHEMA IF NOT EXISTS bronze_{schema_prefix};

CREATE TABLE IF NOT EXISTS bronze_{schema_prefix}.risk_events (
    event_id      TEXT        NOT NULL,
    customer_id   TEXT        NOT NULL,
    event_type    TEXT,
    severity      TEXT,
    occurred_at   TIMESTAMPTZ,
    resolved_at   TIMESTAMPTZ,
    notes         TEXT,
    row_hash      TEXT        NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_run_id TEXT        NOT NULL,
    PRIMARY KEY (event_id)
);
