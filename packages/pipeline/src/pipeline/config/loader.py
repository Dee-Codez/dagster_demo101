from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings

_CONFIG_DIR = Path(__file__).parents[3] / "config" / "clients"

_ENV_RE = re.compile(r"\$\{([^}]+)\}")


def _expand_env_vars(raw: Any) -> Any:
    """Recursively interpolate ${VAR} references in YAML values."""
    if isinstance(raw, str):
        return _ENV_RE.sub(lambda m: os.environ.get(m.group(1), m.group(0)), raw)
    if isinstance(raw, dict):
        return {k: _expand_env_vars(v) for k, v in raw.items()}
    if isinstance(raw, list):
        return [_expand_env_vars(i) for i in raw]
    return raw


class SourceConfig(BaseModel):
    connector_type: str
    dsn: str
    queries: list[str]

    @field_validator("connector_type")
    @classmethod
    def _check_connector(cls, v: str) -> str:
        if v not in {"sqlite", "oracle"}:
            raise ValueError(f"connector_type must be sqlite or oracle, got {v!r}")
        return v


class ScheduleConfig(BaseModel):
    bronze_refresh: str


class ClientConfig(BaseModel):
    client_id: str
    display_name: str
    source: SourceConfig
    postgres_schema_prefix: str
    schedules: ScheduleConfig
    pipelines: list[str]


def load_client_config(client_id: str) -> ClientConfig:
    path = _CONFIG_DIR / f"{client_id}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"No config found for client {client_id!r} at {path}")
    raw = yaml.safe_load(path.read_text())
    raw = _expand_env_vars(raw)
    return ClientConfig(client_id=client_id, **raw)


def list_clients() -> list[str]:
    return sorted(p.stem for p in _CONFIG_DIR.glob("*.yaml"))
