"""Acceso mínimo al warehouse (Postgres), independiente de Airflow para poder testear."""
import os
from contextlib import contextmanager

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def url() -> str:
    return os.environ["ER_WAREHOUSE_URL"]


@contextmanager
def connect():
    conn = psycopg2.connect(url())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(sql: str, params=None) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)


def read_df(sql: str, params=None) -> pd.DataFrame:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def scalar(sql: str, params=None):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def insert_df(table: str, df: pd.DataFrame, on_conflict: str = "") -> None:
    """Inserta un DataFrame; NaN/NaT se escriben como NULL."""
    if df.empty:
        return
    clean = df.astype(object).where(pd.notna(df), None)
    cols = ", ".join(clean.columns)
    sql = f"INSERT INTO {table} ({cols}) VALUES %s {on_conflict}"
    with connect() as conn, conn.cursor() as cur:
        execute_values(cur, sql, list(clean.itertuples(index=False, name=None)), page_size=1000)
