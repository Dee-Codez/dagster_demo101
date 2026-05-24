from __future__ import annotations

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import Engine, text


class PostgresWriter:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def execute_ddl(self, sql: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(text(sql))

    def upsert(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        conflict_col: str,
        change_guard_col: str | None = "row_hash",
    ) -> int:
        """Stage df then merge into target. Returns rows inserted or updated."""
        staging = f"_staging_{table}"
        with self._engine.begin() as conn:
            df.to_sql(staging, conn, schema=schema, if_exists="replace", index=False, method="multi")

            # Query actual target schema so we can cast columns to their declared types.
            # This avoids TEXT vs TIMESTAMPTZ mismatches from pandas' inference on the staging table.
            ts_result = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = :s AND table_name = :t "
                    "AND data_type IN ('timestamp with time zone', 'timestamp without time zone')"
                ),
                {"s": schema, "t": table},
            )
            ts_set = {row[0] for row in ts_result}

            cols = list(df.columns)
            col_list = ", ".join(f'"{c}"' for c in cols)
            select_list = ", ".join(
                f'"{c}"::TIMESTAMPTZ' if c in ts_set else f'"{c}"' for c in cols
            )
            set_cols = [c for c in cols if c != conflict_col]
            set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in set_cols)

            where = (
                f'WHERE "{schema}"."{table}".{change_guard_col} '
                f"IS DISTINCT FROM EXCLUDED.{change_guard_col}"
                if change_guard_col and change_guard_col in df.columns
                else ""
            )

            result = conn.execute(
                text(
                    f'INSERT INTO "{schema}"."{table}" ({col_list}) '
                    f"SELECT {select_list} FROM \"{schema}\".\"{staging}\" "
                    f"ON CONFLICT ({conflict_col}) "
                    f"DO UPDATE SET {set_clause} "
                    f"{where}"
                )
            )
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging}"'))
            return result.rowcount

    def replace(self, df: pd.DataFrame, schema: str, table: str) -> None:
        """Full replacement — used for silver and gold layers."""
        with self._engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}"'))
        with self._engine.begin() as conn:
            df.to_sql(table, conn, schema=schema, if_exists="append", index=False, method="multi")


class PostgresReader:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def read(self, schema: str, table: str) -> pd.DataFrame:
        with self._engine.connect() as conn:
            return pd.read_sql(f'SELECT * FROM "{schema}"."{table}"', conn)

    def scalar(self, sql: str) -> object:
        with self._engine.connect() as conn:
            return conn.execute(text(sql)).scalar()
