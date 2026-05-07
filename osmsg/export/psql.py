"""PostgreSQL exporter via DuckDB's postgres extension.

No new Python dep — DuckDB attaches the target Postgres database, mirrors the
osmsg schema, and runs `INSERT … SELECT` so the same DuckDB → Postgres copy
benefits from streaming. The tables created on the Postgres side mirror the
osmsg DuckDB schema, which makes both backends queryable identically.
"""

from __future__ import annotations

import duckdb

from ..pg_schema import PG_SCHEMA


def to_psql(conn: duckdb.DuckDBPyConnection, dsn: str) -> None:
    """Push every osmsg table into the libpq DSN target. DSN must be trusted (ATTACH interpolation)."""
    conn.execute("INSTALL postgres")
    conn.execute("LOAD postgres")
    safe_dsn = dsn.replace("'", "''")
    conn.execute(f"ATTACH '{safe_dsn}' AS pg_target (TYPE postgres)")
    try:
        for stmt in PG_SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(f"CALL postgres_execute('pg_target', $${stmt}$$)")

        # Tables with natural primary keys: ON CONFLICT DO NOTHING is a no-op safety net.
        for table in ("users", "changesets", "changeset_stats"):
            conn.execute(f"INSERT INTO pg_target.{table} SELECT * FROM {table} ON CONFLICT DO NOTHING")

        # state is single-row-per-source: UPSERT to mirror the DuckDB-side truth.
        conn.execute(
            """
            INSERT INTO pg_target.state (source_url, last_seq, last_ts, updated_at)
            SELECT source_url, last_seq, last_ts, updated_at FROM state
            ON CONFLICT (source_url) DO UPDATE SET
                last_seq   = EXCLUDED.last_seq,
                last_ts    = EXCLUDED.last_ts,
                updated_at = EXCLUDED.updated_at
            """
        )
    finally:
        conn.execute("DETACH pg_target")


__all__ = ["PG_SCHEMA", "to_psql"]
