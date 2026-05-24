from __future__ import annotations

import pandas as pd
import pytest

from pipeline.assets.transforms import cleanse, deduplicate, fill_nulls, normalise_column_names


def _customers_df(**overrides) -> pd.DataFrame:
    rows = [
        {
            "customer_id": "CUST000001",
            "full_name": "Jane Doe",
            "segment": "retail",
            "onboarded_at": "2020-01-15T00:00:00+00:00",
            "country": "GB",
            "email": "jane@example.com",
            "row_hash": "abc123",
            "ingested_at": "2024-01-01T00:00:00+00:00",
            "source_run_id": "run-001",
        }
    ]
    rows[0].update(overrides)
    return pd.DataFrame(rows)


def test_normalise_drops_nothing():
    df = _customers_df()
    result = normalise_column_names(df)
    assert set(result.columns) == set(df.columns)


def test_normalise_camel_case():
    df = pd.DataFrame([{"CustomerID": "1", "fullName": "x", "onboardedAt": "y"}])
    result = normalise_column_names(df)
    assert "customer_i_d" in result.columns or "customer_id" in result.columns
    assert "full_name" in result.columns
    assert "onboarded_at" in result.columns


def test_deduplicate_keeps_latest():
    df = pd.DataFrame([
        {"customer_id": "C1", "segment": "retail", "ingested_at": "2024-01-01T00:00:00+00:00"},
        {"customer_id": "C1", "segment": "premium", "ingested_at": "2024-06-01T00:00:00+00:00"},
    ])
    result = deduplicate(df, "customer_id")
    assert len(result) == 1
    assert result.iloc[0]["segment"] == "premium"


def test_deduplicate_no_duplicates_unchanged():
    df = pd.DataFrame([
        {"customer_id": "C1", "segment": "retail", "ingested_at": "2024-01-01T00:00:00+00:00"},
        {"customer_id": "C2", "segment": "premium", "ingested_at": "2024-01-01T00:00:00+00:00"},
    ])
    assert len(deduplicate(df, "customer_id")) == 2


def test_fill_nulls_amount():
    df = pd.DataFrame([{"txn_id": "T1", "amount": None, "currency": "GBP"}])
    result = fill_nulls(df)
    assert result.iloc[0]["amount"] == 0.0


def test_fill_nulls_ignores_non_numeric():
    df = pd.DataFrame([{"customer_id": "C1", "full_name": None}])
    result = fill_nulls(df)
    assert pd.isna(result.iloc[0]["full_name"])


def test_cleanse_drops_bronze_cols():
    df = _customers_df()
    result = cleanse(df, "customer_id")
    assert "row_hash" not in result.columns
    assert "ingested_at" not in result.columns
    assert "source_run_id" not in result.columns


def test_cleanse_preserves_key_col():
    df = _customers_df()
    result = cleanse(df, "customer_id")
    assert "customer_id" in result.columns


def test_cleanse_no_duplicate_keys():
    df = pd.concat([_customers_df(), _customers_df()], ignore_index=True)
    result = cleanse(df, "customer_id")
    assert result["customer_id"].nunique() == result["customer_id"].count()
