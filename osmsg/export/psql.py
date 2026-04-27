"""PostgreSQL exporter via DuckDB's postgres extension.

No new Python dep — DuckDB attaches the target Postgres database, mirrors the
osmsg schema, and runs `INSERT … SELECT` so the same DuckDB → Postgres copy
benefits from streaming. The tables created on the Postgres side mirror the
osmsg DuckDB schema, which makes both backends queryable identically.
"""

from __future__ import annotations

import duckdb

PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    uid      BIGINT PRIMARY KEY,
    username TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS changesets (
    changeset_id BIGINT PRIMARY KEY,
    uid          BIGINT NOT NULL REFERENCES users(uid),
    created_at   TIMESTAMPTZ,
    hashtags     TEXT[],
    editor       TEXT,
    min_lon      DOUBLE PRECISION,
    min_lat      DOUBLE PRECISION,
    max_lon      DOUBLE PRECISION,
    max_lat      DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_changesets_created_at ON changesets(created_at);
CREATE TABLE IF NOT EXISTS changeset_stats (
    changeset_id   BIGINT NOT NULL REFERENCES changesets(changeset_id),
    seq_id         BIGINT NOT NULL,
    uid            BIGINT NOT NULL REFERENCES users(uid),
    nodes_created  INTEGER DEFAULT 0,
    nodes_modified INTEGER DEFAULT 0,
    nodes_deleted  INTEGER DEFAULT 0,
    ways_created   INTEGER DEFAULT 0,
    ways_modified  INTEGER DEFAULT 0,
    ways_deleted   INTEGER DEFAULT 0,
    rels_created   INTEGER DEFAULT 0,
    rels_modified  INTEGER DEFAULT 0,
    rels_deleted   INTEGER DEFAULT 0,
    poi_created    INTEGER DEFAULT 0,
    poi_modified   INTEGER DEFAULT 0,
    tag_stats      JSONB,
    PRIMARY KEY (seq_id, changeset_id)
);
CREATE INDEX IF NOT EXISTS idx_changeset_stats_uid ON changeset_stats(uid);
CREATE TABLE IF NOT EXISTS state (
    source_url  TEXT PRIMARY KEY,
    last_seq    BIGINT NOT NULL,
    last_ts     TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);
"""


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
