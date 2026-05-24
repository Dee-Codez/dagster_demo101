from pathlib import Path

from dagster import AssetKey, asset

from ..config.loader import ClientConfig
from ..resources.db import PostgresResource

_SQL_DIR = Path(__file__).parents[3] / "sql" / "gold"


def _run_gold_sql(postgres: PostgresResource, schema_prefix: str, sql_file: str) -> None:
    sql = (_SQL_DIR / sql_file).read_text().replace("{schema_prefix}", schema_prefix)
    postgres.execute_ddl(sql)


def make_gold_assets(cfg: ClientConfig) -> list:
    prefix = cfg.postgres_schema_prefix
    silver_deps = [AssetKey(f"silver_{prefix}_{t}") for t in cfg.pipelines]

    @asset(
        name=f"gold_{prefix}_risk_customer_features",
        group_name=f"gold_{prefix}",
        deps=silver_deps,
    )
    def risk_features(context, postgres: PostgresResource) -> None:
        _run_gold_sql(postgres, prefix, "risk_customer_features.sql")
        count = postgres.reader.scalar(
            f'SELECT COUNT(*) FROM "gold_{prefix}"."risk_customer_features"'
        )
        context.add_output_metadata({"rows": count})

    out = [risk_features]

    if set(cfg.pipelines) >= {"customers", "accounts", "transactions", "risk_events"}:

        @asset(
            name=f"gold_{prefix}_analytics_monthly",
            group_name=f"gold_{prefix}",
            deps=silver_deps,
        )
        def analytics_monthly(context, postgres: PostgresResource) -> None:
            _run_gold_sql(postgres, prefix, "analytics_monthly.sql")
            count = postgres.reader.scalar(
                f'SELECT COUNT(*) FROM "gold_{prefix}"."analytics_monthly"'
            )
            context.add_output_metadata({"rows": count})

        out.append(analytics_monthly)

    return out
