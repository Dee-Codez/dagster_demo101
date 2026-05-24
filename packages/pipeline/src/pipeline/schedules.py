from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from .config.loader import ClientConfig


def make_bronze_schedule(cfg: ClientConfig) -> ScheduleDefinition:
    prefix = cfg.postgres_schema_prefix
    job = define_asset_job(
        name=f"bronze_refresh_{prefix}",
        selection=AssetSelection.groups(f"bronze_{prefix}"),
    )
    return ScheduleDefinition(
        job=job,
        cron_schedule=cfg.schedules.bronze_refresh,
        name=f"bronze_refresh_{prefix}_schedule",
    )
