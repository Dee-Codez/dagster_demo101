# connectors

Reusable Python library for database connectivity. No Dagster dependency — can be used standalone or published to a private registry.

## Install

```bash
pip install -e .
# or via the workspace root:
poetry install
```

## What's in here

| Module | Purpose |
|--------|---------|
| `base.py` | `SourceConnector` ABC — extract a named query as a DataFrame |
| `sqlite_connector.py` | Local dev mock. Reads from a SQLite file with the same interface as Oracle |
| `oracle_connector.py` | Real Oracle connector. Requires `oracledb` and Oracle client libraries |
| `postgres.py` | `PostgresWriter` (upsert with CDC guard, full replace) + `PostgresReader` |
| `__init__.py` | `get_connector(connector_type, dsn)` factory |

## Swapping connectors

The factory `get_connector("sqlite", dsn)` and `get_connector("oracle", dsn)` both return a `SourceConnector`. The rest of the platform only depends on `SourceConnector.extract(query_name)`.

To add a new source (e.g. MySQL, REST API): subclass `SourceConnector`, implement `extract` and `close`, register in `get_connector`.

## Adding a new Oracle query

Edit `_ORACLE_QUERIES` in `oracle_connector.py` and add the same key to `_QUERIES` in `sqlite_connector.py`. Both must expose the same set of query names.

## Setting up real Oracle

```bash
pip install oracledb
# Oracle Instant Client must be on LD_LIBRARY_PATH / PATH
```

Then set `connector_type: oracle` and supply a valid `dsn` in the client YAML.

## What we'd do differently with more time

- Batch extraction with server-side cursors for tables > 50M rows
- Connection pooling via SQLAlchemy's built-in pool rather than creating a new engine per call
- Retry logic with exponential backoff for transient Oracle connectivity failures
- A proper `async` variant for high-throughput ingestion
