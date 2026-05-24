# pipeline

Dagster project implementing Bronze → Silver → Gold for the financial data platform.

## Running locally

```bash
# from the workspace root:
make seed          # generate source data into data/source.db
make dev           # start Dagster UI at http://localhost:3000
```

Or directly:

```bash
cd packages/pipeline
dagster dev -m pipeline.definitions
```

## Project structure

```
src/pipeline/
├── assets/
│   ├── bronze.py       # bronze asset factory + CDC asset checks
│   ├── silver.py       # silver asset factory (cleanse + full refresh)
│   ├── gold.py         # gold asset factory (pure SQL aggregations)
│   └── transforms.py   # pure cleanse() function — no Dagster dependency
├── resources/
│   └── db.py           # PostgresResource (ConfigurableResource)
├── config/
│   └── loader.py       # Pydantic config loader + ${ENV_VAR} interpolation
├── schedules.py        # per-client bronze refresh schedule
├── sensors.py          # silver freshness asset checks
└── definitions.py      # Definitions factory — iterates all client configs
config/clients/
├── acme.yaml
└── demo.yaml
sql/
├── bronze/*.sql        # idempotent DDL (CREATE TABLE IF NOT EXISTS)
├── silver/*.sql        # idempotent DDL
└── gold/*.sql          # DROP + CREATE TABLE AS SELECT (full rebuild)
scripts/
└── seed_source.py      # Faker → SQLite source data
```

## Data layers

### Bronze
Raw landing zone. One Postgres schema per client (`bronze_acme`, `bronze_demo`, …). Schema is created by the asset itself on first run via idempotent DDL. Every row carries:
- `row_hash` — SHA-256 of all non-key payload columns, used for CDC
- `ingested_at` — timestamp of when the row was last changed in bronze
- `source_run_id` — Dagster run ID that wrote this row

**Change detection:** full extract from source, hash each row, upsert with `ON CONFLICT (pk) DO UPDATE ... WHERE row_hash IS DISTINCT FROM EXCLUDED.row_hash`. Rows that haven't changed are untouched.

### Silver
Cleansed layer in `silver_<client>` schema. Rebuilt fully on each run from bronze. Transforms applied by `transforms.cleanse()`:
- Column names normalised to snake_case
- `amount` and `balance` null → 0.0
- Duplicates on business key removed (latest `ingested_at` wins)
- Timestamps coerced to UTC
- Bronze-internal columns (`row_hash`, `ingested_at`, `source_run_id`) stripped
- Pipeline metadata added: `silver_loaded_at`, `pipeline_version`, `source_system`

### Gold
Serving layer in `gold_<client>` schema. Two outputs:

**`risk_customer_features`** — one row per customer with RFM-style signals for a scoring model: transaction recency/frequency/monetary over 30d and 90d windows, account count and balance aggregates, open risk event count, max severity score.

**`analytics_monthly`** — one row per `(customer_id, year_month)` with monthly transaction aggregates, new accounts opened, and risk event counts. Joinable across all four source domains.

Gold SQL is in `sql/gold/` and does a full `DROP + CREATE TABLE AS SELECT` each run.

## Orchestration

- **Schedule**: each client has a bronze refresh schedule driven by `schedules.bronze_refresh` in its YAML (`acme` runs at 02:00 UTC daily).
- **Asset checks**: bronze tables checked for non-zero row count after materialisation; silver tables checked for freshness (warn if `silver_loaded_at` > 25 hours ago).
- **Asset graph**: bronze → silver → gold dependencies enforced by Dagster via `deps=`.

## Client configuration

Each file in `config/clients/*.yaml` defines one client. The platform discovers clients at import time via `list_clients()`.

```yaml
display_name: ACME Financial
source:
  connector_type: sqlite   # or: oracle
  dsn: "sqlite:///data/source.db"
  queries: [customers, accounts, transactions, risk_events]
postgres_schema_prefix: acme
schedules:
  bronze_refresh: "0 2 * * *"
pipelines: [customers, accounts, transactions, risk_events]
```

DSN values support `${ENV_VAR}` interpolation.

## What we'd do differently with more time

- Dagster I/O managers to decouple assets from the storage layer entirely
- Per-asset resource binding so multiple clients can run with different source credentials simultaneously in one `Definitions` object
- Incremental silver: filter bronze by `source_run_id` matching the current run rather than full rebuild
- Partitioned assets by date for large historical backfills
- Great Expectations checks wired as Dagster asset checks in the silver layer
- `dagster-dbt` integration if dbt were allowed (it isn't here — noted for future)
