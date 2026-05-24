from __future__ import annotations

from datetime import datetime, timezone

from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

from .config.loader import ClientConfig
from .resources.db import PostgresResource

_FRESHNESS_THRESHOLD_HOURS = 25


def make_silver_freshness_checks(cfg: ClientConfig) -> list:
    checks_out = []
    prefix = cfg.postgres_schema_prefix

    def _make_check(table: str):
        silver_schema = f"silver_{prefix}"

        @asset_check(
            asset=AssetKey(f"silver_{prefix}_{table}"),
            name=f"silver_{prefix}_{table}_freshness",
            required_resource_keys={"postgres"},
        )
        def _check(postgres: PostgresResource) -> AssetCheckResult:
            result = postgres.reader.scalar(
                f'SELECT MAX(silver_loaded_at) FROM "{silver_schema}"."{table}"'
            )

            if result is None:
                return AssetCheckResult(
                    passed=False,
                    severity=AssetCheckSeverity.WARN,
                    metadata={"reason": "table empty or does not exist"},
                )

            max_loaded = result if result.tzinfo else result.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - max_loaded).total_seconds() / 3600

            return AssetCheckResult(
                passed=age_hours < _FRESHNESS_THRESHOLD_HOURS,
                severity=AssetCheckSeverity.WARN,
                metadata={
                    "age_hours": round(age_hours, 2),
                    "threshold_hours": _FRESHNESS_THRESHOLD_HOURS,
                },
            )

        return _check

    for table in cfg.pipelines:
        checks_out.append(_make_check(table))

    return checks_out
