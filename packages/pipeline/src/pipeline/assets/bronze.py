from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, AssetExecutionContext, asset, asset_check

from connectors import get_connector

from ..config.loader import ClientConfig
from ..resources.db import PostgresResource

_SQL_DIR = Path(__file__).parents[3] / "sql" / "bronze"

_BUSINESS_KEYS: dict[str, str] = {
    "customers": "customer_id",
    "accounts": "account_id",
    "transactions": "txn_id",
    "risk_events": "event_id",
}


def _compute_row_hash(df: pd.DataFrame, key_col: str) -> pd.Series:
    payload_cols = sorted(c for c in df.columns if c != key_col)
    return (
        df[payload_cols]
        .astype(str)
        .apply(lambda row: hashlib.sha256("|".join(row).encode()).hexdigest(), axis=1)
    )


def _ensure_table(postgres: PostgresResource, schema_prefix: str, table: str) -> None:
    ddl = (_SQL_DIR / f"{table}.sql").read_text().replace("{schema_prefix}", schema_prefix)
    postgres.execute_ddl(ddl)


def make_bronze_assets(cfg: ClientConfig) -> list:
    assets_out = []
    prefix = cfg.postgres_schema_prefix

    def _make_asset(table: str, key_col: str):
        @asset(
            name=f"bronze_{prefix}_{table}",
            group_name=f"bronze_{prefix}",
            required_resource_keys={"postgres"},
        )
        def _bronze(context: AssetExecutionContext, postgres: PostgresResource) -> None:
            _ensure_table(postgres, cfg.postgres_schema_prefix, table)

            with get_connector(cfg.source.connector_type, cfg.source.dsn) as conn:
                df = conn.extract(table)

            df["row_hash"] = _compute_row_hash(df, key_col)
            df["source_run_id"] = context.run_id
            df["ingested_at"] = pd.Timestamp.utcnow()

            schema = f"bronze_{cfg.postgres_schema_prefix}"
            rows_changed = postgres.writer.upsert(
                df, schema=schema, table=table, conflict_col=key_col
            )
            context.add_output_metadata(
                {"rows_total": len(df), "rows_changed": rows_changed}
            )

        return _bronze

    for table in cfg.pipelines:
        assets_out.append(_make_asset(table, _BUSINESS_KEYS[table]))

    return assets_out


def make_bronze_checks(cfg: ClientConfig) -> list:
    checks_out = []
    prefix = cfg.postgres_schema_prefix

    def _make_check(table: str):
        schema = f"bronze_{prefix}"

        @asset_check(
            asset=f"bronze_{prefix}_{table}",
            name=f"bronze_{prefix}_{table}_row_count",
            required_resource_keys={"postgres"},
        )
        def _check(postgres: PostgresResource) -> AssetCheckResult:
            count = postgres.reader.scalar(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            return AssetCheckResult(
                passed=count > 0,
                severity=AssetCheckSeverity.ERROR,
                metadata={"row_count": count},
            )

        return _check

    for table in cfg.pipelines:
        checks_out.append(_make_check(table))

    return checks_out
