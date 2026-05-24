from __future__ import annotations

import sqlalchemy as sa
from dagster import ConfigurableResource
from sqlalchemy import Engine

from connectors.postgres import PostgresReader, PostgresWriter


class PostgresResource(ConfigurableResource):
    dsn: str

    @property
    def engine(self) -> Engine:
        return sa.create_engine(self.dsn)

    @property
    def writer(self) -> PostgresWriter:
        return PostgresWriter(self.engine)

    @property
    def reader(self) -> PostgresReader:
        return PostgresReader(self.engine)

    def ensure_schema(self, schema_name: str) -> None:
        # CREATE SCHEMA IF NOT EXISTS has a known race under concurrent execution.
        # Running it in isolation and swallowing the duplicate error is the safe path.
        try:
            with self.engine.begin() as conn:
                conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        except Exception:
            pass

    def execute_ddl(self, sql: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(sa.text(sql))
