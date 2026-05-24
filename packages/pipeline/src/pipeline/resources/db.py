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

    def execute_ddl(self, sql: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(sa.text(sql))
