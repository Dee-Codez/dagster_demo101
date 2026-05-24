from __future__ import annotations

import os

from dagster import Definitions

from .assets.bronze import make_bronze_assets, make_bronze_checks
from .assets.gold import make_gold_assets
from .assets.silver import make_silver_assets
from .config.loader import list_clients, load_client_config
from .resources.db import PostgresResource
from .schedules import make_bronze_schedule
from .sensors import make_silver_freshness_checks

_PG_DSN = os.environ.get(
    "PLATFORM_PG_DSN",
    "postgresql://platform:platform@localhost/platform",
)

all_assets: list = []
all_checks: list = []
all_schedules: list = []

_CLIENT_FILTER = os.environ.get("PLATFORM_CLIENT_ID")

for _client_id in list_clients():
    if _CLIENT_FILTER and _client_id != _CLIENT_FILTER:
        continue
    _cfg = load_client_config(_client_id)

    _bronze = make_bronze_assets(_cfg)
    _silver = make_silver_assets(_cfg)
    _gold = make_gold_assets(_cfg)

    all_assets.extend(_bronze + _silver + _gold)
    all_checks.extend(make_bronze_checks(_cfg))
    all_checks.extend(make_silver_freshness_checks(_cfg))
    all_schedules.append(make_bronze_schedule(_cfg))

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    schedules=all_schedules,
    resources={
        "postgres": PostgresResource(dsn=_PG_DSN),
    },
)
