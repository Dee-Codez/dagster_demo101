from __future__ import annotations

import pytest
from pydantic import ValidationError

from pipeline.config.loader import list_clients, load_client_config


def test_list_clients_returns_known_clients():
    clients = list_clients()
    assert "acme" in clients
    assert "demo" in clients


def test_load_acme_config():
    cfg = load_client_config("acme")
    assert cfg.client_id == "acme"
    assert cfg.postgres_schema_prefix == "acme"
    assert "customers" in cfg.pipelines
    assert cfg.source.connector_type == "sqlite"


def test_load_demo_config():
    cfg = load_client_config("demo")
    assert cfg.client_id == "demo"
    assert cfg.postgres_schema_prefix == "demo"
    assert cfg.schedules.bronze_refresh == "0 6 * * *"


def test_missing_client_raises():
    with pytest.raises(FileNotFoundError):
        load_client_config("does_not_exist")


def test_invalid_connector_type_raises(tmp_path, monkeypatch):
    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("""
display_name: Bad Client
source:
  connector_type: kafka
  dsn: "sqlite:///x.db"
  queries: [customers]
postgres_schema_prefix: bad
schedules:
  bronze_refresh: "0 * * * *"
pipelines: [customers]
""")
    import pipeline.config.loader as loader_mod
    monkeypatch.setattr(loader_mod, "_CONFIG_DIR", tmp_path)
    with pytest.raises(ValidationError, match="connector_type"):
        load_client_config("bad")


def test_env_var_interpolation(monkeypatch, tmp_path):
    monkeypatch.setenv("MY_DSN", "sqlite:///injected.db")
    yaml_file = tmp_path / "envtest.yaml"
    yaml_file.write_text("""
display_name: Env Test
source:
  connector_type: sqlite
  dsn: "${MY_DSN}"
  queries: [customers]
postgres_schema_prefix: envtest
schedules:
  bronze_refresh: "0 1 * * *"
pipelines: [customers]
""")
    import pipeline.config.loader as loader_mod
    monkeypatch.setattr(loader_mod, "_CONFIG_DIR", tmp_path)
    cfg = load_client_config("envtest")
    assert cfg.source.dsn == "sqlite:///injected.db"
