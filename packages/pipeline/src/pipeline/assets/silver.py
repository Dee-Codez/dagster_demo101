from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from dagster import AssetKey, asset

from ..config.loader import ClientConfig
from ..resources.db import PostgresResource
from .transforms import cleanse

_SQL_DIR = Path(__file__).parents[3] / "sql" / "silver"

_BUSINESS_KEYS: dict[str, str] = {
    "customers": "customer_id",
    "accounts": "account_id",
    "transactions": "txn_id",
    "risk_events": "event_id",
}

_PIPELINE_VERSION = "0.1.0"


def _ensure_table(postgres: PostgresResource, schema_prefix: str, table: str) -> None:
    schema = f"silver_{schema_prefix}"
    postgres.ensure_schema(schema)
    full_ddl = (_SQL_DIR / f"{table}.sql").read_text().replace("{schema_prefix}", schema_prefix)
    table_ddl = "\n".join(
        line for line in full_ddl.splitlines()
        if not line.startswith("CREATE SCHEMA")
    )
    postgres.execute_ddl(table_ddl)


def make_silver_assets(cfg: ClientConfig) -> list:
    assets_out = []
    prefix = cfg.postgres_schema_prefix

    def _make_asset(table: str, key_col: str):
        bronze_schema = f"bronze_{prefix}"
        silver_schema = f"silver_{prefix}"

        @asset(
            name=f"silver_{prefix}_{table}",
            group_name=f"silver_{prefix}",
            deps=[AssetKey(f"bronze_{prefix}_{table}")],
        )
        def _silver(context, postgres: PostgresResource) -> None:
            _ensure_table(postgres, cfg.postgres_schema_prefix, table)

            df = postgres.reader.read(schema=bronze_schema, table=table)
            df = cleanse(df, key_col)

            df["silver_loaded_at"] = pd.Timestamp.now(tz="UTC")
            df["pipeline_version"] = _PIPELINE_VERSION
            df["source_system"] = f"oracle_{cfg.postgres_schema_prefix}"

            engine = postgres.engine
            with engine.begin() as conn:
                conn.execute(sa.text(f'TRUNCATE TABLE "{silver_schema}"."{table}"'))
            with engine.begin() as conn:
                df.to_sql(table, conn, schema=silver_schema, if_exists="append", index=False, method="multi")

            context.add_output_metadata({"rows_written": len(df)})

        return _silver

    for table in cfg.pipelines:
        assets_out.append(_make_asset(table, _BUSINESS_KEYS[table]))

    return assets_out
