import hashlib
from pathlib import Path

import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, asset, asset_check

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


def _parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if col.endswith("_at") and df[col].dtype == object:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def _ensure_table(postgres: PostgresResource, schema_prefix: str, table: str) -> None:
    schema = f"bronze_{schema_prefix}"
    postgres.ensure_schema(schema)
    # SQL file still contains CREATE SCHEMA line — skip it, only run the CREATE TABLE block
    full_ddl = (_SQL_DIR / f"{table}.sql").read_text().replace("{schema_prefix}", schema_prefix)
    table_ddl = "\n".join(
        line for line in full_ddl.splitlines()
        if not line.startswith("CREATE SCHEMA")
    )
    postgres.execute_ddl(table_ddl)


def make_bronze_assets(cfg: ClientConfig) -> list:
    assets_out = []
    prefix = cfg.postgres_schema_prefix

    def _make_asset(table: str, key_col: str):
        @asset(name=f"bronze_{prefix}_{table}", group_name=f"bronze_{prefix}")
        def _bronze(context, postgres: PostgresResource) -> None:
            _ensure_table(postgres, cfg.postgres_schema_prefix, table)

            with get_connector(cfg.source.connector_type, cfg.source.dsn) as conn:
                df = conn.extract(table)

            df = _parse_timestamps(df)
            df["row_hash"] = _compute_row_hash(df, key_col)
            df["source_run_id"] = context.run_id
            df["ingested_at"] = pd.Timestamp.now(tz="UTC")

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

        @asset_check(asset=f"bronze_{prefix}_{table}", name=f"bronze_{prefix}_{table}_row_count")
        def _check(postgres: PostgresResource) -> AssetCheckResult:
            try:
                count = postgres.reader.scalar(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            except Exception:
                return AssetCheckResult(
                    passed=False,
                    severity=AssetCheckSeverity.WARN,
                    metadata={"reason": "table does not exist — materialise the asset first"},
                )
            return AssetCheckResult(
                passed=count > 0,
                severity=AssetCheckSeverity.ERROR,
                metadata={"row_count": count},
            )

        return _check

    for table in cfg.pipelines:
        checks_out.append(_make_check(table))

    return checks_out
