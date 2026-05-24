from __future__ import annotations

import os

import pytest
import sqlalchemy as sa

_PG_DSN = os.environ.get(
    "TEST_PG_DSN",
    "postgresql://platform:platform@localhost/platform",
)


@pytest.fixture(scope="session")
def pg_engine():
    try:
        engine = sa.create_engine(_PG_DSN)
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        yield engine
        engine.dispose()
    except Exception:
        pytest.skip("Postgres not available — run: docker compose up -d")


@pytest.fixture(scope="session")
def pg_test_schema(pg_engine):
    schema = "test_platform"
    with pg_engine.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    yield schema
    with pg_engine.begin() as conn:
        conn.execute(sa.text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
