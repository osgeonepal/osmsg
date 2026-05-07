from __future__ import annotations

from typing import Any

import duckdb

from .duckdb_schema import DUCKDB_SCHEMA


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def close(conn: duckdb.DuckDBPyConnection) -> None:
    conn.close()


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for stmt in DUCKDB_SCHEMA.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)


def upsert_state(conn: duckdb.DuckDBPyConnection, *, source_url: str, last_seq: int, last_ts, updated_at) -> None:
    conn.execute(
        """
        INSERT INTO state (source_url, last_seq, last_ts, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (source_url) DO UPDATE SET
            last_seq   = EXCLUDED.last_seq,
            last_ts    = EXCLUDED.last_ts,
            updated_at = EXCLUDED.updated_at
        """,
        [source_url, last_seq, last_ts, updated_at],
    )


def get_state(conn: duckdb.DuckDBPyConnection, source_url: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT last_seq, last_ts, updated_at FROM state WHERE source_url = ?",
        [source_url],
    ).fetchone()
    if row is None:
        return None
    return {"last_seq": row[0], "last_ts": row[1], "updated_at": row[2]}


__all__ = ["close", "connect", "create_tables", "get_state", "upsert_state"]
