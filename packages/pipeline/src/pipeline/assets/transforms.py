from __future__ import annotations

import re

import pandas as pd

_NUMERIC_FILL_COLS = {"amount", "balance"}
_BRONZE_INTERNAL = {"row_hash", "ingested_at", "source_run_id"}
_TIMESTAMP_SUFFIX = "_at"


def normalise_column_names(df: pd.DataFrame) -> pd.DataFrame:
    def to_snake(name: str) -> str:
        s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
        return s.lower().strip().replace(" ", "_").replace("-", "_")

    return df.rename(columns={c: to_snake(c) for c in df.columns})


def deduplicate(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    if "ingested_at" in df.columns:
        df = df.sort_values("ingested_at", ascending=False)
    return df.drop_duplicates(subset=[key_col], keep="first")


def fill_nulls(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if col in _NUMERIC_FILL_COLS:
            df[col] = df[col].fillna(0.0).infer_objects(copy=False)
    return df


def coerce_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if col.endswith(_TIMESTAMP_SUFFIX) and df[col].dtype == object:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def cleanse(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    df = normalise_column_names(df)
    df = deduplicate(df, key_col)
    df = fill_nulls(df)
    df = coerce_timestamps(df)
    df = df.drop(columns=[c for c in _BRONZE_INTERNAL if c in df.columns])
    return df.reset_index(drop=True)
