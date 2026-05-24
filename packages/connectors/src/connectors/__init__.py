from .base import SourceConnector
from .oracle_connector import OracleConnector
from .postgres import PostgresReader, PostgresWriter
from .sqlite_connector import SqliteMockConnector


def get_connector(connector_type: str, dsn: str) -> SourceConnector:
    match connector_type:
        case "sqlite":
            return SqliteMockConnector(dsn)
        case "oracle":
            return OracleConnector(dsn)
        case _:
            raise ValueError(f"Unknown connector type: {connector_type!r}")


__all__ = [
    "SourceConnector",
    "SqliteMockConnector",
    "OracleConnector",
    "PostgresWriter",
    "PostgresReader",
    "get_connector",
]
