# Data Platform

A local financial services data platform: Oracle → Bronze → Silver → Gold, orchestrated with Dagster, served from Postgres.

## Quick start

```bash
# 1. Start Postgres
docker compose up -d

# 2. Install all dependencies
poetry install

# 3. Generate source data (~2 000 customers, ~3 000 accounts, ~60 000 transactions)
make seed

# 4. Launch Dagster UI
make dev
# open http://localhost:3000
```

In the Dagster UI, materialise assets in order: bronze → silver → gold. Or run the full pipeline via **Materialise all**.

## Repo layout

```
data-platform/
├── packages/
│   ├── connectors/      # installable connector library (Oracle, SQLite mock, Postgres)
│   └── pipeline/        # Dagster project (assets, resources, schedules, sensors)
├── docker-compose.yml
├── Makefile
└── pyproject.toml       # Poetry workspace root
```

## Running tests

```bash
# Unit tests only (no Postgres needed)
poetry run pytest packages/ -v -m "not integration"

# Full suite (requires docker compose up -d)
make test
```

## Key design decisions

**Monorepo with clear package boundaries** — `connectors` is an installable library with its own `pyproject.toml`. It has no Dagster dependency and could be published to a private registry. `pipeline` depends on it as a path install.

**Bronze CDC via row-hash** — the platform doesn't assume the Oracle source has a reliable `updated_at` column. Instead, every extract computes `SHA-256(sorted_payload_columns)` and uses Postgres `ON CONFLICT ... DO UPDATE ... WHERE row_hash IS DISTINCT FROM EXCLUDED.row_hash`. Unchanged rows are not touched. This is efficient for the stated constraint of ~50 changed rows per day out of millions.

**Silver as full rebuild** — silver reads from bronze (already in Postgres) and rewrites the full table each run. This is cheap because no data crosses network boundaries. The incremental CDC work is already done at the bronze layer. Silver could be made incremental by filtering bronze rows where `source_run_id = current_run_id`, but full rebuild keeps the logic simpler.

**Gold via pure SQL** — aggregations in `sql/gold/*.sql` use CTEs and Postgres window functions. No pandas. The SQL is the source of truth for what the gold tables contain, making it easy to audit and debug.

**Multi-client via YAML + Pydantic** — drop a new file in `packages/pipeline/config/clients/<name>.yaml` and restart `dagster dev`. All bronze/silver/gold assets for that client are created automatically. No code changes needed.

**One Postgres resource, all clients** — all clients share the same Postgres instance with schema-level isolation (`bronze_acme`, `silver_acme`, `gold_acme`, `bronze_demo`, …). The `PostgresResource` DSN comes from `PLATFORM_PG_DSN` env var.

## Adding a new client

1. Create `packages/pipeline/config/clients/<client_id>.yaml`
2. Ensure source data is accessible at the DSN specified in the YAML
3. Restart `dagster dev`

The new client's asset groups appear automatically.

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `PLATFORM_PG_DSN` | `postgresql://platform:platform@localhost/platform` | Postgres connection |
| `TEST_PG_DSN` | same as above | Used by integration tests |

## What we'd do differently with more time

- Schema versioning with Alembic or Flyway instead of idempotent `CREATE TABLE IF NOT EXISTS`
- Dagster I/O managers instead of direct SQLAlchemy calls inside assets
- Per-asset resource binding to support multiple live clients running simultaneously without restarting
- Great Expectations or Soda data quality checks wired as asset checks in the silver layer
- Incremental silver materialisation (filter bronze by `source_run_id` of the current Dagster run)
- Docker image for the Dagster daemon + webserver so the whole platform runs in Compose
- Oracle integration tested against Oracle XE (available as a Docker image)
- Column-level lineage via OpenLineage/Marquez
