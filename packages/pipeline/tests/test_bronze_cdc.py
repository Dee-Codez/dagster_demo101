"""Critical path: row-hash CDC must not update unchanged rows."""
from __future__ import annotations

import hashlib

import pandas as pd
import pytest
import sqlalchemy as sa
from sqlalchemy import text

from connectors.postgres import PostgresWriter
from pipeline.assets.bronze import _compute_row_hash


# ── pure unit tests (no DB) ────────────────────────────────────────────────

def _make_df(overrides: dict | None = None) -> pd.DataFrame:
    row = {
        "customer_id": "CUST000001",
        "full_name": "Jane Doe",
        "segment": "retail",
        "onboarded_at": "2020-01-15T00:00:00+00:00",
        "country": "GB",
        "email": "jane@example.com",
    }
    if overrides:
        row.update(overrides)
    return pd.DataFrame([row])


def test_row_hash_is_deterministic():
    df = _make_df()
    h1 = _compute_row_hash(df, "customer_id").iloc[0]
    h2 = _compute_row_hash(df, "customer_id").iloc[0]
    assert h1 == h2


def test_row_hash_changes_on_mutation():
    h_before = _compute_row_hash(_make_df(), "customer_id").iloc[0]
    h_after = _compute_row_hash(_make_df({"segment": "premium"}), "customer_id").iloc[0]
    assert h_before != h_after


def test_row_hash_excludes_key_column():
    df_a = _make_df({"customer_id": "CUST000001"})
    df_b = _make_df({"customer_id": "CUST999999"})
    # same payload, different key → same hash
    assert (
        _compute_row_hash(df_a, "customer_id").iloc[0]
        == _compute_row_hash(df_b, "customer_id").iloc[0]
    )


def test_row_hash_length():
    h = _compute_row_hash(_make_df(), "customer_id").iloc[0]
    assert len(h) == 64  # SHA-256 hex


# ── integration tests (real Postgres required) ─────────────────────────────

@pytest.mark.integration
def test_cdc_no_update_on_identical_data(pg_engine, pg_test_schema):
    schema = pg_test_schema
    writer = PostgresWriter(pg_engine)

    with pg_engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.cdc_customers (
                customer_id   TEXT NOT NULL PRIMARY KEY,
                full_name     TEXT,
                segment       TEXT,
                row_hash      TEXT NOT NULL,
                ingested_at   TIMESTAMPTZ DEFAULT NOW(),
                source_run_id TEXT NOT NULL
            )
        """))
        conn.execute(text(f"TRUNCATE {schema}.cdc_customers"))

    df = _make_df()
    df["row_hash"] = _compute_row_hash(df, "customer_id")
    df["source_run_id"] = "run-001"
    df["ingested_at"] = pd.Timestamp.utcnow()

    first = writer.upsert(df, schema=schema, table="cdc_customers", conflict_col="customer_id")
    assert first == 1

    df["source_run_id"] = "run-002"
    second = writer.upsert(df, schema=schema, table="cdc_customers", conflict_col="customer_id")
    assert second == 0, "identical row must not count as changed"


@pytest.mark.integration
def test_cdc_updates_changed_row(pg_engine, pg_test_schema):
    schema = pg_test_schema
    writer = PostgresWriter(pg_engine)

    with pg_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {schema}.cdc_customers"))

    df = _make_df()
    df["row_hash"] = _compute_row_hash(df, "customer_id")
    df["source_run_id"] = "run-A"
    df["ingested_at"] = pd.Timestamp.utcnow()
    writer.upsert(df, schema=schema, table="cdc_customers", conflict_col="customer_id")

    mutated = _make_df({"segment": "premium"})
    mutated["row_hash"] = _compute_row_hash(mutated, "customer_id")
    mutated["source_run_id"] = "run-B"
    mutated["ingested_at"] = pd.Timestamp.utcnow()
    changed = writer.upsert(mutated, schema=schema, table="cdc_customers", conflict_col="customer_id")
    assert changed == 1

    with pg_engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT segment, source_run_id FROM {schema}.cdc_customers WHERE customer_id = 'CUST000001'")
        ).fetchone()
    assert row.segment == "premium"
    assert row.source_run_id == "run-B"
