# connectors

Reusable Python library for database connectivity. Has no Dagster dependency — can be used in any Python context or published to a private registry independently of the pipeline package.

---

## Table of contents

- [Install](#install)
- [Package structure](#package-structure)
- [Source connectors](#source-connectors)
  - [SQLite (mock Oracle)](#sqlite-mock-oracle)
  - [Oracle](#oracle)
  - [Swapping connectors](#swapping-connectors)
  - [Adding a new source](#adding-a-new-source)
- [Postgres writer](#postgres-writer)
  - [Upsert with CDC guard](#upsert-with-cdc-guard)
  - [Full replace](#full-replace)
- [Postgres reader](#postgres-reader)
- [get_connector factory](#get_connector-factory)

---

## Install

From the workspace root (recommended — installs into the shared venv):

```bash
poetry install
```

Standalone:

```bash
pip install -e packages/connectors
```

---

## Package structure

```
src/connectors/
├── __init__.py          # get_connector() factory
├── base.py              # SourceConnector abstract base class
├── sqlite_connector.py  # SQLite mock (same interface as Oracle)
├── oracle_connector.py  # Oracle connector (requires oracledb)
└── postgres.py          # PostgresWriter + PostgresReader
```

---

## Source connectors

All source connectors implement the same `SourceConnector` ABC from `base.py`:

```python
class SourceConnector:
    def extract(self, query_name: str) -> pd.DataFrame: ...
    def close(self) -> None: ...
    def __enter__(self): ...
    def __exit__(self, *args): ...
```

`extract(query_name)` returns a DataFrame with the full result of the named query. The caller never writes SQL — it only passes a name like `"customers"` or `"transactions"`.

### SQLite (mock Oracle)

Used for local development. Reads from a SQLite file using queries that mirror the Oracle table structure.

```python
from connectors import get_connector

with get_connector("sqlite", "sqlite:///data/source.db") as conn:
    df = conn.extract("customers")   # returns DataFrame
    df = conn.extract("transactions")
```

Supported query names: `customers`, `accounts`, `transactions`, `risk_events`.

The SQLite connector raises `ValueError` for unrecognised query names.

### Oracle

For production use against a real Oracle database. Uses `oracledb` (the official Oracle Python driver) which is imported lazily — the connector can be imported without `oracledb` installed, it only fails at instantiation time.

```python
from connectors import get_connector

dsn = "oracle://user:pass@host:1521/ORCL"
with get_connector("oracle", dsn) as conn:
    df = conn.extract("customers")
```

**Setup for Oracle:**

```bash
pip install oracledb
# Oracle Instant Client must be on LD_LIBRARY_PATH (Linux) or PATH (Windows)
# See: https://python-oracledb.readthedocs.io/en/latest/user_guide/installation.html
```

Then in the client YAML:

```yaml
source:
  connector_type: oracle
  dsn: "oracle://${ORACLE_USER}:${ORACLE_PASS}@host:1521/SID"
```

### Swapping connectors

The pipeline only depends on `SourceConnector.extract(query_name)`. The connector type is configured per-client in YAML. Switching from SQLite to Oracle for a client requires only changing two lines in its YAML — no Python changes.

### Adding a new source

1. Subclass `SourceConnector` in a new file:

```python
from .base import SourceConnector
import pandas as pd

class MyConnector(SourceConnector):
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def extract(self, query_name: str) -> pd.DataFrame:
        # implement extraction logic
        ...

    def close(self) -> None:
        ...
```

2. Register it in `__init__.py`'s `get_connector` factory:

```python
case "mytype":
    from .my_connector import MyConnector
    return MyConnector(dsn)
```

3. Add the same query names to `_QUERIES` / `_ORACLE_QUERIES` in both the SQLite and Oracle connectors so they stay in sync for testing.

---

## Postgres writer

`PostgresWriter` handles all writes to Postgres. It is instantiated with a SQLAlchemy `Engine`.

```python
from connectors.postgres import PostgresWriter
import sqlalchemy as sa

engine = sa.create_engine("postgresql://platform:platform@localhost/platform")
writer = PostgresWriter(engine)
```

### Upsert with CDC guard

Used by the bronze layer. Stages the DataFrame into a temporary `_staging_<table>` table, then merges into the target using `ON CONFLICT ... DO UPDATE ... WHERE row_hash IS DISTINCT FROM EXCLUDED.row_hash`.

```python
rows_changed = writer.upsert(
    df,
    schema="bronze_acme",
    table="customers",
    conflict_col="customer_id",         # primary key / business key
    change_guard_col="row_hash",        # only update if this column changed
)
# rows_changed: number of rows inserted or updated (unchanged rows return 0)
```

**How the change guard works:**

The upsert query includes a `WHERE` clause that compares the stored `row_hash` with the incoming one:

```sql
INSERT INTO bronze_acme.customers (...)
SELECT ... FROM bronze_acme._staging_customers
ON CONFLICT (customer_id)
DO UPDATE SET ...
WHERE bronze_acme.customers.row_hash IS DISTINCT FROM EXCLUDED.row_hash
```

If the row hash matches (content unchanged), Postgres skips the update and the row is not touched. `rowcount` reflects only actually written rows.

**Type safety:** before the INSERT, the method queries `information_schema.columns` to identify which target columns are declared as timestamp types, then casts those columns explicitly to `TIMESTAMPTZ` in the SELECT. This avoids TEXT vs. TIMESTAMPTZ mismatches that arise from pandas' staging table type inference.

### Full replace

Used by the `PostgresWriter.replace()` method (available for silver/gold if needed). Truncates the target table then bulk-inserts the DataFrame:

```python
writer.replace(df, schema="silver_acme", table="customers")
```

---

## Postgres reader

`PostgresReader` handles all reads from Postgres.

```python
from connectors.postgres import PostgresReader

reader = PostgresReader(engine)

# Read full table as DataFrame
df = reader.read(schema="bronze_acme", table="customers")

# Execute a scalar query (returns a single value)
count = reader.scalar('SELECT COUNT(*) FROM "bronze_acme"."customers"')
```

---

## get_connector factory

`get_connector(connector_type, dsn)` is the single entry point for source connectors. Returns a context manager.

```python
from connectors import get_connector

# SQLite mock
with get_connector("sqlite", "sqlite:///data/source.db") as conn:
    df = conn.extract("customers")

# Oracle
with get_connector("oracle", "oracle://user:pass@host/SID") as conn:
    df = conn.extract("customers")
```

Raises `ValueError` for unrecognised `connector_type` values.
