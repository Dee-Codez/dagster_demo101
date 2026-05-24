from __future__ import annotations

import pandas as pd
import pytest
import sqlalchemy as sa
from sqlalchemy import text

from connectors.postgres import PostgresReader, PostgresWriter


@pytest.fixture
def scratch_schema(pg_engine):
    schema = "test_connectors"
    with pg_engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    yield schema
    with pg_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


@pytest.fixture
def simple_table(pg_engine, scratch_schema):
    with pg_engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {scratch_schema}.widgets (
                widget_id TEXT PRIMARY KEY,
                name      TEXT,
                quantity  INT,
                row_hash  TEXT NOT NULL
            )
        """))
        conn.execute(text(f"TRUNCATE {scratch_schema}.widgets"))
    return scratch_schema


def _widget_df(widget_id="W1", name="Sprocket", qty=10, row_hash="hash_a"):
    return pd.DataFrame([{
        "widget_id": widget_id,
        "name": name,
        "quantity": qty,
        "row_hash": row_hash,
    }])


def test_upsert_inserts_new_row(pg_engine, simple_table):
    writer = PostgresWriter(pg_engine)
    count = writer.upsert(_widget_df(), simple_table, "widgets", "widget_id")
    assert count == 1


def test_upsert_skips_identical_row(pg_engine, simple_table):
    writer = PostgresWriter(pg_engine)
    writer.upsert(_widget_df(), simple_table, "widgets", "widget_id")
    count = writer.upsert(_widget_df(), simple_table, "widgets", "widget_id")
    assert count == 0


def test_upsert_updates_changed_row(pg_engine, simple_table):
    writer = PostgresWriter(pg_engine)
    writer.upsert(_widget_df(row_hash="hash_a"), simple_table, "widgets", "widget_id")
    count = writer.upsert(_widget_df(name="Bolt", row_hash="hash_b"), simple_table, "widgets", "widget_id")
    assert count == 1


def test_reader_returns_dataframe(pg_engine, simple_table):
    writer = PostgresWriter(pg_engine)
    writer.upsert(_widget_df(), simple_table, "widgets", "widget_id")
    reader = PostgresReader(pg_engine)
    df = reader.read(simple_table, "widgets")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_reader_scalar(pg_engine, simple_table):
    writer = PostgresWriter(pg_engine)
    writer.upsert(_widget_df(), simple_table, "widgets", "widget_id")
    reader = PostgresReader(pg_engine)
    count = reader.scalar(f"SELECT COUNT(*) FROM {simple_table}.widgets")
    assert count == 1
