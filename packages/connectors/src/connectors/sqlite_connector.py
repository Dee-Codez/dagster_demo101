from __future__ import annotations

import pandas as pd
import sqlalchemy as sa

from .base import SourceConnector

_QUERIES: dict[str, str] = {
    "customers": "SELECT * FROM customers",
    "accounts": "SELECT * FROM accounts",
    "transactions": "SELECT * FROM transactions",
    "risk_events": "SELECT * FROM risk_events",
}


class SqliteMockConnector(SourceConnector):
    def __init__(self, dsn: str) -> None:
        self._engine = sa.create_engine(dsn)

    def extract(self, query_name: str) -> pd.DataFrame:
        if query_name not in _QUERIES:
            raise ValueError(f"Unknown query: {query_name!r}")
        with self._engine.connect() as conn:
            return pd.read_sql(sa.text(_QUERIES[query_name]), conn)

    def close(self) -> None:
        self._engine.dispose()
