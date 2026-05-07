"""PostgreSQL exporter via DuckDB's postgres extension."""

import duckdb

from ..pg_schema import PG_SCHEMA


def to_psql(conn: duckdb.DuckDBPyConnection, dsn: str) -> None:
    """Push every osmsg table to the libpq DSN target.

    DSN must be trusted — it is interpolated directly into the ATTACH statement.
    """
    conn.execute("INSTALL postgres")
    conn.execute("LOAD postgres")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    safe_dsn = dsn.replace("'", "''")
    conn.execute(f"ATTACH '{safe_dsn}' AS pg_target (TYPE postgres)")
    try:
        for stmt in PG_SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(f"CALL postgres_execute('pg_target', $${stmt}$$)")

        conn.execute("INSERT INTO pg_target.users SELECT * FROM users ON CONFLICT DO NOTHING")

        # Mirrors the DuckDB-side merge: newer non-NULL wins, NULL never downgrades.
        conn.execute(
            """
            INSERT INTO pg_target.changesets AS c (changeset_id, uid, created_at, hashtags, editor, geom)
            SELECT changeset_id, uid, created_at, hashtags, editor, geom FROM changesets
            ON CONFLICT (changeset_id) DO UPDATE SET
                created_at = COALESCE(EXCLUDED.created_at, c.created_at),
                hashtags   = COALESCE(EXCLUDED.hashtags,   c.hashtags),
                editor     = COALESCE(EXCLUDED.editor,     c.editor),
                geom       = COALESCE(EXCLUDED.geom,       c.geom)
            """
        )

        conn.execute("INSERT INTO pg_target.changeset_stats SELECT * FROM changeset_stats ON CONFLICT DO NOTHING")

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
