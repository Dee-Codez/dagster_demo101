from __future__ import annotations

import pandas as pd

from .base import SourceConnector

# Maps the same logical query names used by SqliteMockConnector to the real
# Oracle SQL that was provided by the client.
_ORACLE_QUERIES: dict[str, str] = {
    "customers": """
        SELECT c.customer_id, c.full_name, c.segment,
               c.onboarded_at, c.country, c.email
        FROM   client_schema.customers c
        WHERE  c.active = 1
    """,
    "accounts": """
        SELECT a.account_id, a.customer_id, a.account_type,
               a.balance, a.currency, a.opened_at, a.status, a.closed_at
        FROM   client_schema.accounts a
    """,
    "transactions": """
        SELECT t.txn_id, t.account_id, t.amount, t.currency,
               t.txn_type, t.txn_at, t.description, t.status
        FROM   client_schema.transactions t
    """,
    "risk_events": """
        SELECT r.event_id, r.customer_id, r.event_type,
               r.severity, r.occurred_at, r.resolved_at, r.notes
        FROM   client_schema.risk_events r
    """,
}


class OracleConnector(SourceConnector):
    """Requires Oracle client libraries and the oracledb package."""

    def __init__(self, dsn: str) -> None:
        import oracledb  # deferred — not installed in local dev

        self._conn = oracledb.connect(dsn=dsn)

    def extract(self, query_name: str) -> pd.DataFrame:
        if query_name not in _ORACLE_QUERIES:
            raise ValueError(f"Unknown query: {query_name!r}")
        return pd.read_sql(_ORACLE_QUERIES[query_name], self._conn)

    def close(self) -> None:
        self._conn.close()
