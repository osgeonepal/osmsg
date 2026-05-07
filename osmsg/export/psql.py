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

        _copy = "INSERT INTO pg_target.{t} SELECT * FROM {t} ON CONFLICT DO NOTHING"
        for table in ("users", "changesets", "changeset_stats"):
            conn.execute(_copy.format(t=table))

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
