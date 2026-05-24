# Data Platform

A local financial services data platform that simulates an Oracle source system and moves data through a three-tier Postgres warehouse (Bronze → Silver → Gold), orchestrated by Dagster. Built as a monorepo with clean package boundaries and multi-client support.

---

## Table of contents

- [Architecture overview](#architecture-overview)
- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [First-time setup](#first-time-setup)
- [Starting the platform](#starting-the-platform)
- [Using the Dagster UI](#using-the-dagster-ui)
- [Running a single client](#running-a-single-client)
- [Verifying data in Postgres](#verifying-data-in-postgres)
- [Running tests](#running-tests)
- [Adding a new client](#adding-a-new-client)
- [Environment variables](#environment-variables)
- [Data layers in detail](#data-layers-in-detail)
- [Orchestration](#orchestration)
- [Multi-client design](#multi-client-design)
- [Design decisions](#design-decisions)

---

## Architecture overview

```
Oracle (simulated via SQLite)
        │
        │  full extract every run
        ▼
┌─────────────────────────────┐
│  Bronze  (bronze_<client>)  │  raw landing, CDC via row-hash upsert
└─────────────────────────────┘
        │
        │  full rebuild from bronze
        ▼
┌─────────────────────────────┐
│  Silver  (silver_<client>)  │  cleansed, normalised, deduplicated
└─────────────────────────────┘
        │
        │  pure-SQL aggregation
        ▼
┌─────────────────────────────┐
│  Gold    (gold_<client>)    │  risk features + monthly analytics
└─────────────────────────────┘
```

All three layers live in the same Postgres instance, isolated by schema. Each client gets its own set of schemas (e.g. `bronze_acme`, `silver_acme`, `gold_acme`).

---

## Repository layout

```
data-platform/
├── packages/
│   ├── connectors/                  # standalone connector library (no Dagster dep)
│   │   └── src/connectors/
│   │       ├── base.py              # SourceConnector ABC
│   │       ├── sqlite_connector.py  # SQLite mock (simulates Oracle)
│   │       ├── oracle_connector.py  # real Oracle connector stub
│   │       ├── postgres.py          # PostgresWriter + PostgresReader
│   │       └── __init__.py          # get_connector() factory
│   └── pipeline/                    # Dagster project
│       ├── src/pipeline/
│       │   ├── assets/
│       │   │   ├── bronze.py        # bronze asset + CDC row-count checks
│       │   │   ├── silver.py        # silver asset (cleanse + full refresh)
│       │   │   ├── gold.py          # gold asset (pure SQL)
│       │   │   └── transforms.py    # pure cleanse() — no Dagster dep
│       │   ├── resources/
│       │   │   └── db.py            # PostgresResource (ConfigurableResource)
│       │   ├── config/
│       │   │   └── loader.py        # Pydantic config + ${ENV_VAR} interpolation
│       │   ├── schedules.py         # per-client bronze refresh schedule
│       │   ├── sensors.py           # silver freshness asset checks
│       │   └── definitions.py       # Definitions entry point
│       ├── config/clients/
│       │   ├── acme.yaml            # ACME Financial (full 4-table pipeline)
│       │   └── demo.yaml            # Demo Corp (2-table subset)
│       ├── sql/
│       │   ├── bronze/*.sql         # idempotent CREATE TABLE IF NOT EXISTS DDL
│       │   ├── silver/*.sql         # idempotent CREATE TABLE IF NOT EXISTS DDL
│       │   └── gold/*.sql           # DROP + CREATE TABLE AS SELECT
│       └── scripts/
│           └── seed_source.py       # Faker → SQLite source data
├── docker-compose.yml               # Postgres 16
├── Makefile                         # common commands
├── pyproject.toml                   # Poetry workspace root
└── .env.example                     # copy to .env and fill in
```

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.11 – 3.14 | 3.11 recommended |
| Poetry | 1.8+ | `pip install poetry` |
| Docker | any recent | for Postgres |
| Docker Compose | v2 | bundled with Docker Desktop |

---

## First-time setup

```bash
# 1. Clone and enter the workspace root
cd data-platform

# 2. Copy the example env file and adjust if needed
cp .env.example .env

# 3. Start Postgres in the background
make up

# 4. Install all Python dependencies (both packages in one venv)
make install

# 5. Seed the SQLite source database
#    Generates: ~2 000 customers, ~3 000 accounts, ~60 000 transactions, ~500 risk events
make seed
```

The seed script writes to `data/source.db` relative to the workspace root. This file is what the `sqlite` connector reads from.

---

## Starting the platform

```bash
# Start Dagster UI (all clients)
make dev
# open http://localhost:3000

# Start Dagster UI for a specific client only
make dev-acme
```

Dagster discovers all client configs at import time. If `PLATFORM_CLIENT_ID` is set, only that client's assets are registered.

To stop Dagster: `Ctrl+C` in the terminal running `make dev`.  
To stop Postgres: `make down`.

---

## Using the Dagster UI

Open `http://localhost:3000` after `make dev`.

### Materialising assets (running the pipeline)

Assets must be materialised in dependency order: **bronze → silver → gold**.

**Option A — by group (recommended for first run):**
1. Click **Assets** in the left sidebar
2. Filter by group (e.g. `bronze_acme`) using the group filter at the top
3. Select all assets in the group
4. Click **Materialise selected**
5. Repeat for `silver_acme`, then `gold_acme`

**Option B — materialise all:**
1. Click **Assets** → **Materialise all**
2. Dagster respects the dependency graph and runs layers in the correct order

**Option C — individual asset:**
1. Click any asset in the catalog
2. Click the **Materialise** button in the top-right corner

### Reading run output

After a run completes, click the run ID in **Runs** to see:
- Per-step logs
- Asset metadata (e.g. `rows_total`, `rows_changed` for bronze; `rows_written` for silver)
- Asset check results (row count checks, freshness checks)

### Asset checks

Bronze assets have a **row count check** — fails if the table is empty after materialisation.  
Silver assets have a **freshness check** — warns if `silver_loaded_at` is more than 25 hours old.

These appear under the **Checks** tab of each asset and as a summary badge on the asset card.

### Schedules

Each client has a bronze refresh schedule defined in its YAML. ACME runs at 02:00 UTC daily. To enable a schedule:
1. Click **Automation** in the left sidebar
2. Find the schedule (e.g. `bronze_acme_refresh`)
3. Toggle it on

---

## Running a single client

```bash
# In the terminal (affects which assets appear in the UI)
make dev-acme

# Or inline
PLATFORM_CLIENT_ID=acme make dev
```

Without `PLATFORM_CLIENT_ID`, all clients in `config/clients/` are loaded.

---

## Verifying data in Postgres

```bash
# Open a psql shell
docker exec -it $(docker ps -qf "ancestor=postgres:16") psql -U platform

# Check row counts per layer
SELECT COUNT(*) FROM bronze_acme.customers;
SELECT COUNT(*) FROM silver_acme.customers;
SELECT COUNT(*) FROM gold_acme.risk_customer_features;
SELECT COUNT(*) FROM gold_acme.analytics_monthly;

# Sample gold output
SELECT customer_id, segment, txn_count_30d, open_risk_events, max_severity_score
FROM gold_acme.risk_customer_features
LIMIT 10;
```

Expected rough counts after seeding and full materialisation:

| Table | Approximate rows |
|-------|-----------------|
| `bronze_acme.customers` | 2 000 |
| `bronze_acme.accounts` | 3 000 |
| `bronze_acme.transactions` | 60 000 |
| `bronze_acme.risk_events` | 500 |
| `silver_acme.*` | same as bronze (minus duplicates) |
| `gold_acme.risk_customer_features` | ~2 000 (one per customer) |
| `gold_acme.analytics_monthly` | varies by date range |

---

## Running tests

```bash
# Unit tests — no Postgres needed
poetry run pytest packages/ -v -m "not integration"

# Full test suite — requires Postgres running (make up)
make test
```

---

## Adding a new client

1. Create `packages/pipeline/config/clients/<client_id>.yaml`:

```yaml
display_name: My Client
source:
  connector_type: sqlite        # or: oracle
  dsn: "sqlite:///data/source.db"
  queries: [customers, accounts, transactions, risk_events]
postgres_schema_prefix: myclient
schedules:
  bronze_refresh: "0 3 * * *"  # cron expression
pipelines: [customers, accounts, transactions, risk_events]
```

2. Ensure the source database is accessible at the specified DSN.
3. Restart `make dev` — the new client's asset groups appear automatically. No code changes needed.

DSN values support `${ENV_VAR}` interpolation, e.g. `dsn: "oracle://${ORACLE_USER}:${ORACLE_PASS}@host/SID"`.

---

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `PLATFORM_PG_DSN` | `postgresql://platform:platform@localhost/platform` | Postgres connection string for all assets |
| `PLATFORM_CLIENT_ID` | _(all clients)_ | If set, only this client's assets are loaded |
| `TEST_PG_DSN` | same as `PLATFORM_PG_DSN` | Used by integration tests |

Set these in `.env` (loaded automatically by Poetry/shell) or export them before running `make dev`.

---

## Data layers in detail

### Bronze

**Purpose:** raw landing zone — exact copy of source data, nothing transformed.

**How it works:**
1. Full extract from the source system (all rows, every run)
2. Parse timestamps — columns ending in `_at` that are ISO strings get coerced to `TIMESTAMPTZ`
3. Compute `row_hash` — SHA-256 of all non-key payload columns concatenated with `|` as separator
4. Upsert into Postgres with a change guard:

```sql
ON CONFLICT (customer_id)
DO UPDATE SET ...
WHERE bronze_acme.customers.row_hash IS DISTINCT FROM EXCLUDED.row_hash
```

Only rows whose content has actually changed are written. Unchanged rows are skipped entirely.

**Columns added by bronze:**
- `row_hash TEXT` — SHA-256 fingerprint of row content
- `ingested_at TIMESTAMPTZ` — when this row was last modified in bronze
- `source_run_id TEXT` — Dagster run ID that last touched this row

### Silver

**Purpose:** cleansed, analyst-ready version of bronze. Full rebuild on every run.

**Transformations applied (in order):**
1. Column names normalised to snake_case (handles camelCase and PascalCase from Oracle)
2. Deduplicate on business key — keeps the row with the latest `ingested_at`
3. Null-fill `amount` and `balance` columns with `0.0`
4. Coerce any remaining string `_at` columns to UTC datetime
5. Strip bronze-internal columns (`row_hash`, `ingested_at`, `source_run_id`)

**Columns added by silver:**
- `silver_loaded_at TIMESTAMPTZ` — when this row was written to silver
- `pipeline_version TEXT` — code version at time of write (e.g. `0.1.0`)
- `source_system TEXT` — source identifier (e.g. `oracle_acme`)

### Gold

**Purpose:** aggregated serving layer for risk scoring and analytics. Pure SQL, no Python transforms.

**`risk_customer_features`** — one row per customer:
- `onboarded_days_ago` — customer tenure
- `account_count`, `total_balance`, `avg_balance`
- `txn_count_30d`, `txn_count_90d` — transaction frequency windows
- `txn_amount_sum_90d`, `txn_amount_avg_90d` — monetary value
- `days_since_last_txn` — recency signal
- `open_risk_events` — unresolved risk events
- `max_severity_score` — 0–4 score mapped from low/medium/high/critical

**`analytics_monthly`** — one row per `(customer_id, year_month)`:
- Monthly transaction count, sum, and average
- New accounts opened that month
- Risk events opened and resolved that month

Gold tables are fully rebuilt every run via `DROP TABLE IF EXISTS ... CREATE TABLE AS SELECT`.

---

## Orchestration

Dagster manages the entire pipeline as a software-defined asset graph.

- **Dependencies** are declared via `deps=` on each asset — Dagster enforces bronze-before-silver-before-gold ordering
- **Schedules** are defined per-client in their YAML; the `schedules.bronze_refresh` cron expression maps directly to a `ScheduleDefinition` targeting the `bronze_<client>` asset group
- **Asset checks** run automatically after materialisation:
  - Bronze: row count must be > 0
  - Silver: `silver_loaded_at` must be within 25 hours
- **Multi-process executor** — Dagster runs assets in parallel within the same layer (all four bronze tables materialise concurrently)

---

## Multi-client design

Each client is isolated at the Postgres schema level. All clients share one Postgres instance and one `PostgresResource`. Adding a client requires only a new YAML file — no Python changes.

At startup, `definitions.py` calls `list_clients()` which scans `config/clients/*.yaml`, loads each with `load_client_config()`, and calls the asset factory functions (`make_bronze_assets`, `make_silver_assets`, `make_gold_assets`) for each. The resulting assets are registered in a single `Definitions` object.

Schema naming convention:

| Layer | Schema pattern | Example |
|-------|---------------|---------|
| Bronze | `bronze_{prefix}` | `bronze_acme` |
| Silver | `silver_{prefix}` | `silver_acme` |
| Gold | `gold_{prefix}` | `gold_acme` |

---

## Design decisions

**Row-hash CDC at bronze** — the platform does not assume the Oracle source exposes a reliable `updated_at` column or change stream. Instead, every row is fingerprinted with SHA-256 and the Postgres upsert only writes rows that have actually changed. This means we can do full extracts (simple, robust) while paying only for actual changes in write I/O.

**Silver as full rebuild** — silver reads from bronze (already in Postgres, no network crossing) and rewrites the whole table each run. This keeps silver logic simple and correct. The incremental work is done at bronze. Silver could be made incremental by filtering on `source_run_id`, but the added complexity is not justified at this scale.

**Gold via pure SQL** — all aggregation logic lives in `sql/gold/*.sql`. No pandas or Python in the gold layer. SQL is auditable, diffable, and easy to hand to an analyst. The Python asset is just the executor that splits the SQL on `;` and runs each statement.

**Monorepo with two packages** — `connectors` has no Dagster dependency and could be published to a private registry and reused in non-Dagster contexts (e.g. ad-hoc scripts, notebooks). `pipeline` depends on `connectors` as a path install.

**One venv, run from root** — `poetry run dagster dev` is invoked from the workspace root, not from within either sub-package. This ensures both packages are installed into the same virtualenv and `import connectors` works inside Dagster's multiprocess worker subprocesses.
