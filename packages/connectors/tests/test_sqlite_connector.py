from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
import sqlalchemy as sa

from connectors import get_connector
from connectors.sqlite_connector import SqliteMockConnector


@pytest.fixture(scope="module")
def sqlite_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name

    engine = sa.create_engine(f"sqlite:///{path}")
    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE customers (
                customer_id TEXT, full_name TEXT, segment TEXT,
                onboarded_at TEXT, country TEXT, email TEXT
            )
        """))
        conn.execute(sa.text(
            "INSERT INTO customers VALUES ('C1','Alice','retail','2020-01-01','GB','a@b.com')"
        ))
        conn.execute(sa.text("""
            CREATE TABLE accounts (
                account_id TEXT, customer_id TEXT, account_type TEXT,
                balance REAL, currency TEXT, opened_at TEXT, status TEXT, closed_at TEXT
            )
        """))
        conn.execute(sa.text("""
            CREATE TABLE transactions (
                txn_id TEXT, account_id TEXT, amount REAL, currency TEXT,
                txn_type TEXT, txn_at TEXT, description TEXT, status TEXT
            )
        """))
        conn.execute(sa.text("""
            CREATE TABLE risk_events (
                event_id TEXT, customer_id TEXT, event_type TEXT, severity TEXT,
                occurred_at TEXT, resolved_at TEXT, notes TEXT
            )
        """))
    yield f"sqlite:///{path}"
    Path(path).unlink(missing_ok=True)


def test_extract_returns_dataframe(sqlite_db):
    with SqliteMockConnector(sqlite_db) as conn:
        df = conn.extract("customers")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert "customer_id" in df.columns


def test_extract_correct_columns(sqlite_db):
    with SqliteMockConnector(sqlite_db) as conn:
        df = conn.extract("customers")
    assert set(df.columns) == {"customer_id", "full_name", "segment", "onboarded_at", "country", "email"}


def test_extract_unknown_query_raises(sqlite_db):
    with SqliteMockConnector(sqlite_db) as conn:
        with pytest.raises(ValueError, match="Unknown query"):
            conn.extract("nonexistent_table")


def test_factory_returns_sqlite_connector(sqlite_db):
    conn = get_connector("sqlite", sqlite_db)
    assert isinstance(conn, SqliteMockConnector)
    conn.close()


def test_factory_raises_on_unknown_type():
    with pytest.raises(ValueError, match="Unknown connector type"):
        get_connector("kafka", "dsn://whatever")


def test_context_manager_closes_cleanly(sqlite_db):
    with get_connector("sqlite", sqlite_db) as conn:
        df = conn.extract("accounts")
    assert isinstance(df, pd.DataFrame)
