# pipeline

Dagster project implementing the Bronze → Silver → Gold pipeline for the financial data platform. Reads client configuration from YAML files, auto-generates asset groups per client, and writes to Postgres using the `connectors` package.

---

## Table of contents

- [Running locally](#running-locally)
- [Project structure](#project-structure)
- [Asset layers](#asset-layers)
  - [Bronze](#bronze)
  - [Silver](#silver)
  - [Gold](#gold)
- [Transforms](#transforms)
- [Asset checks](#asset-checks)
- [Schedules](#schedules)
- [Client configuration](#client-configuration)
- [Adding a new client](#adding-a-new-client)
- [SQL files](#sql-files)
- [Postgres resource](#postgres-resource)
- [Definitions entry point](#definitions-entry-point)

---

## Running locally

```bash
# From the workspace root (data-platform/)

# Generate source data first (if not already done)
make seed

# Start Dagster UI — all clients
make dev

# Start Dagster UI — one specific client
make dev-acme
# equivalent: PLATFORM_CLIENT_ID=acme make dev

# open http://localhost:3000
```

Do not run `dagster dev` from inside `packages/pipeline/` — it creates an isolated venv where the `connectors` package is not installed. Always run from the workspace root.

---

## Project structure

```
packages/pipeline/
├── src/pipeline/
│   ├── assets/
│   │   ├── bronze.py        # make_bronze_assets() + make_bronze_checks()
│   │   ├── silver.py        # make_silver_assets()
│   │   ├── gold.py          # make_gold_assets()
│   │   └── transforms.py    # cleanse() — pure functions, no Dagster dep
│   ├── resources/
│   │   └── db.py            # PostgresResource
│   ├── config/
│   │   └── loader.py        # ClientConfig + load_client_config() + list_clients()
│   ├── schedules.py         # make_bronze_schedule()
│   ├── sensors.py           # make_silver_freshness_checks()
│   └── definitions.py       # Definitions entry point
├── config/clients/
│   ├── acme.yaml
│   └── demo.yaml
├── sql/
│   ├── bronze/
│   │   ├── customers.sql
│   │   ├── accounts.sql
│   │   ├── transactions.sql
│   │   └── risk_events.sql
│   ├── silver/
│   │   ├── customers.sql
│   │   ├── accounts.sql
│   │   ├── transactions.sql
│   │   └── risk_events.sql
│   └── gold/
│       ├── risk_customer_features.sql
│       └── analytics_monthly.sql
└── scripts/
    └── seed_source.py       # Faker → SQLite mock source data
```

---

## Asset layers

### Bronze

**File:** [assets/bronze.py](src/pipeline/assets/bronze.py)

One asset per pipeline table per client. Asset names follow `bronze_{prefix}_{table}` (e.g. `bronze_acme_customers`). All assets belong to the `bronze_{prefix}` group.

**Execution flow per asset:**

1. `_ensure_table()` — creates the bronze schema and table if they don't exist (idempotent DDL from `sql/bronze/{table}.sql`)
2. Extract all rows from the source system via `get_connector()`
3. `_parse_timestamps()` — coerce ISO string columns ending in `_at` to timezone-aware datetime
4. `_compute_row_hash()` — SHA-256 of all non-key payload columns joined with `|`
5. Add platform columns: `row_hash`, `source_run_id`, `ingested_at`
6. `postgres.writer.upsert()` — stage to `_staging_{table}`, then `INSERT ON CONFLICT DO UPDATE WHERE row_hash IS DISTINCT FROM EXCLUDED.row_hash`
7. Emit `rows_total` and `rows_changed` as asset metadata

**Platform columns added:**

| Column | Type | Description |
|--------|------|-------------|
| `row_hash` | `TEXT` | SHA-256 of row content — used as CDC change guard |
| `ingested_at` | `TIMESTAMPTZ` | Timestamp of last write to bronze |
| `source_run_id` | `TEXT` | Dagster run ID that wrote this row |

**Change detection:**

The upsert only updates a row when its `row_hash` differs from what is already stored. Rows whose content has not changed since the last run are untouched. This means a full source extract produces minimal write activity — appropriate for sources where only ~50 rows change per day out of millions.

### Silver

**File:** [assets/silver.py](src/pipeline/assets/silver.py)

One asset per pipeline table per client. Asset names follow `silver_{prefix}_{table}`. Each asset declares `deps=[AssetKey(f"bronze_{prefix}_{table}")]` so Dagster enforces bronze-first ordering.

**Execution flow per asset:**

1. `_ensure_table()` — creates the silver schema and table if they don't exist
2. Read the full bronze table via `postgres.reader.read()`
3. `cleanse(df, key_col)` — apply all silver transforms (see [Transforms](#transforms))
4. Add silver metadata columns: `silver_loaded_at`, `pipeline_version`, `source_system`
5. Truncate the silver table
6. Bulk-insert the cleansed DataFrame
7. Emit `rows_written` as asset metadata

Silver is a **full rebuild** every run. Incremental silver (filtering bronze by `source_run_id`) is possible but not implemented — full rebuild keeps the logic simpler.

**Platform columns added:**

| Column | Type | Description |
|--------|------|-------------|
| `silver_loaded_at` | `TIMESTAMPTZ` | Timestamp of this silver write |
| `pipeline_version` | `TEXT` | Code version at time of write |
| `source_system` | `TEXT` | e.g. `oracle_acme` |

### Gold

**File:** [assets/gold.py](src/pipeline/assets/gold.py)

Gold assets are conditionally created based on which pipelines the client has configured:

- `gold_{prefix}_risk_customer_features` — always created
- `gold_{prefix}_analytics_monthly` — only created when all four tables (`customers`, `accounts`, `transactions`, `risk_events`) are present

Both assets declare `deps=silver_deps` (all four silver assets) so they only run after silver is complete.

**Execution flow:**

1. `postgres.ensure_schema(f"gold_{prefix}")` — create schema if not exists
2. Read the SQL file from `sql/gold/{name}.sql`, substitute `{schema_prefix}`
3. Split on `;` and execute each statement in its own transaction
   - Statement 1: `CREATE SCHEMA IF NOT EXISTS gold_{prefix}`
   - Statement 2: `DROP TABLE IF EXISTS gold_{prefix}.{table}`
   - Statement 3: `CREATE TABLE gold_{prefix}.{table} AS SELECT ...`
4. Emit `rows` (post-write count) as asset metadata

Gold SQL runs directly against the silver tables — no pandas involved.

---

## Transforms

**File:** [assets/transforms.py](src/pipeline/assets/transforms.py)

Pure functions with no Dagster dependency. Applied by silver assets via `cleanse(df, key_col)`.

**Pipeline:**

```
cleanse(df, key_col)
  │
  ├─ normalise_column_names(df)
  │    camelCase / PascalCase → snake_case via regex
  │    e.g. customerID → customer_id, fullName → full_name
  │
  ├─ deduplicate(df, key_col)
  │    sort by ingested_at descending → drop_duplicates on business key
  │    latest version of each record wins
  │
  ├─ fill_nulls(df)
  │    amount, balance: NULL → 0.0
  │    all other columns: unchanged
  │
  ├─ coerce_timestamps(df)
  │    columns ending in _at with object dtype → pd.to_datetime(utc=True)
  │
  └─ drop bronze-internal columns
       row_hash, ingested_at, source_run_id are stripped before silver write
```

---

## Asset checks

**Bronze row count check** (`make_bronze_checks` in `bronze.py`):
- One check per bronze asset: `bronze_{prefix}_{table}_row_count`
- Queries `COUNT(*)` after materialisation
- `passed=False, severity=ERROR` if table is empty
- Gracefully handles the case where the table doesn't exist yet (returns `severity=WARN`)

**Silver freshness check** (`make_silver_freshness_checks` in `sensors.py`):
- One check per silver asset: `silver_{prefix}_{table}_freshness`
- Queries `MAX(silver_loaded_at)` and computes age in hours
- `passed=False, severity=WARN` if age > 25 hours
- Gracefully handles missing or empty table

---

## Schedules

**File:** [schedules.py](src/pipeline/schedules.py)

Each client gets one schedule targeting its bronze asset group. The cron expression comes from `schedules.bronze_refresh` in the client YAML.

```python
# acme.yaml: bronze_refresh: "0 2 * * *"
# → runs daily at 02:00 UTC, materialises all bronze_acme assets
```

Schedules are registered in `Definitions` but must be toggled on in the Dagster UI (**Automation** → enable the schedule).

---

## Client configuration

**File:** [config/loader.py](src/pipeline/config/loader.py)

Each file in `config/clients/*.yaml` defines one client. The platform discovers them at import time.

**Full YAML schema:**

```yaml
display_name: ACME Financial          # human-readable name (display only)

source:
  connector_type: sqlite              # "sqlite" or "oracle"
  dsn: "sqlite:///data/source.db"    # connection string; supports ${ENV_VAR}
  queries:                            # which tables to extract
    - customers
    - accounts
    - transactions
    - risk_events

postgres_schema_prefix: acme          # schemas will be bronze_acme, silver_acme, gold_acme

schedules:
  bronze_refresh: "0 2 * * *"        # cron for bronze refresh

pipelines:                            # subset of queries to build assets for
  - customers
  - accounts
  - transactions
  - risk_events
```

**`${ENV_VAR}` interpolation** — any string value in the YAML can reference environment variables:

```yaml
source:
  dsn: "oracle://${ORACLE_USER}:${ORACLE_PASS}@prod-host:1521/FINDB"
```

The loader resolves these at config load time using `os.environ`. A `KeyError` is raised if the variable is not set.

**Pydantic validation** — `ClientConfig` is a Pydantic `BaseModel`. Invalid YAML (missing required fields, wrong types) raises a validation error at startup.

---

## Adding a new client

1. Create `config/clients/<client_id>.yaml` — use `acme.yaml` as a template
2. If using a new source DSN, set the required environment variables
3. Restart `make dev`

The new client's bronze, silver, and gold asset groups appear automatically. For clients with only a subset of tables, the `analytics_monthly` gold asset is omitted (it requires all four tables).

---

## SQL files

SQL files live in `sql/{layer}/{table}.sql`. They use `{schema_prefix}` as a placeholder which is substituted at runtime with the client's `postgres_schema_prefix`.

**Bronze and silver DDL** — idempotent `CREATE TABLE IF NOT EXISTS`. Run once per asset per first materialisation. Schema creation is handled separately by `PostgresResource.ensure_schema()` before the DDL runs (the `CREATE SCHEMA` lines in the SQL files are skipped to avoid a concurrent creation race).

**Gold SQL** — not idempotent by design. Uses `DROP TABLE IF EXISTS` followed by `CREATE TABLE AS SELECT`. This guarantees a clean rebuild every run. Each statement is split on `;` and executed in its own transaction.

---

## Postgres resource

**File:** [resources/db.py](src/pipeline/resources/db.py)

`PostgresResource` is a Dagster `ConfigurableResource` injected into all assets and checks via parameter type annotation.

```python
class PostgresResource(ConfigurableResource):
    dsn: str

    @property
    def engine(self) -> Engine: ...         # creates a SQLAlchemy engine
    @property
    def writer(self) -> PostgresWriter: ... # upsert + replace
    @property
    def reader(self) -> PostgresReader: ... # read + scalar

    def ensure_schema(self, schema_name: str) -> None: ...  # race-safe CREATE SCHEMA
    def execute_ddl(self, sql: str) -> None: ...
```

The DSN is read from `PLATFORM_PG_DSN` (default: `postgresql://platform:platform@localhost/platform`).

`ensure_schema` wraps `CREATE SCHEMA IF NOT EXISTS` in a try/except to handle the Postgres race condition that occurs when multiple assets try to create the same schema simultaneously under the multiprocess executor.

---

## Definitions entry point

**File:** [definitions.py](src/pipeline/definitions.py)

Iterates `list_clients()`, loads each config, calls all factory functions, and registers everything in a single `Definitions` object:

```python
defs = Definitions(
    assets=all_assets,           # bronze + silver + gold for all clients
    asset_checks=all_checks,     # row count + freshness checks for all clients
    schedules=all_schedules,     # one bronze schedule per client
    resources={"postgres": PostgresResource(dsn=_PG_DSN)},
)
```

If `PLATFORM_CLIENT_ID` is set, only that client's definitions are registered. This is useful during development to avoid loading all clients when working on one.
