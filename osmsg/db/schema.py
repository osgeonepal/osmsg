"""DuckDB schema. Three data tables (`users`, `changesets`, `changeset_stats`)
plus a single-row-per-source `state` table for `--update` resume. Identical
schema works in PostgreSQL via the `psql` exporter."""

from __future__ import annotations

from typing import Any

import duckdb


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection at `db_path`. Creates the file if absent."""
    return duckdb.connect(db_path)


def close(conn: duckdb.DuckDBPyConnection) -> None:
    conn.close()


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all osmsg tables if they don't exist. Idempotent."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            uid      BIGINT PRIMARY KEY,
            username VARCHAR NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS changesets (
            changeset_id BIGINT PRIMARY KEY,
            uid          BIGINT NOT NULL REFERENCES users(uid),
            created_at   TIMESTAMPTZ,
            hashtags     VARCHAR[],
            editor       VARCHAR,
            min_lon      DOUBLE,
            min_lat      DOUBLE,
            max_lon      DOUBLE,
            max_lat      DOUBLE
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_changesets_created_at ON changesets(created_at)")

    conn.execute(
        """
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
            tag_stats      JSON,
            PRIMARY KEY (seq_id, changeset_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_changeset_stats_uid ON changeset_stats(uid)")

    # One row per source_url — resume marker, not an audit log.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS state (
            source_url  VARCHAR PRIMARY KEY,
            last_seq    BIGINT NOT NULL,
            last_ts     TIMESTAMPTZ NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL
        )
        """
    )


def upsert_state(conn: duckdb.DuckDBPyConnection, *, source_url: str, last_seq: int, last_ts, updated_at) -> None:
    """Record (or replace) the resume marker for `source_url`. Single row per URL."""
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
    """Return `{last_seq, last_ts, updated_at}` for `source_url`, or None if unseen."""
    row = conn.execute(
        "SELECT last_seq, last_ts, updated_at FROM state WHERE source_url = ?",
        [source_url],
    ).fetchone()
    if row is None:
        return None
    return {"last_seq": row[0], "last_ts": row[1], "updated_at": row[2]}


__all__ = ["close", "connect", "create_tables", "get_state", "upsert_state"]
