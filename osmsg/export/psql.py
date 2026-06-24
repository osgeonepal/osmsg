"""PostgreSQL exporter via DuckDB's postgres extension."""

import duckdb

from ..exceptions import OsmsgError
from ..pg_schema import PG_SCHEMA

# Secondary indexes and foreign keys that make a row-by-row insert slow. For a one-time bulk load
# they are dropped before the COPY and rebuilt once after (one index build + one FK validation,
# instead of maintaining them per row). Primary keys stay, because the ON CONFLICT upserts need them.
# Indexes are (name, create-sql); foreign keys are (table, name, add-clause).
_BULK_INDEXES = [
    ("idx_changesets_created_at", "CREATE INDEX idx_changesets_created_at ON changesets (created_at)"),
    ("idx_changesets_geom", "CREATE INDEX idx_changesets_geom ON changesets USING GIST (geom)"),
    ("idx_changeset_stats_uid", "CREATE INDEX idx_changeset_stats_uid ON changeset_stats (uid)"),
]
_BULK_FKS = [
    ("changesets", "changesets_uid_fkey", "FOREIGN KEY (uid) REFERENCES users (uid)"),
    (
        "changeset_stats",
        "changeset_stats_changeset_id_fkey",
        "FOREIGN KEY (changeset_id) REFERENCES changesets (changeset_id)",
    ),
    ("changeset_stats", "changeset_stats_uid_fkey", "FOREIGN KEY (uid) REFERENCES users (uid)"),
]


# Bulk loads push the big tables in this many changeset_id ranges, each its own statement and so its
# own commit, so a failure costs one range instead of rolling back the whole multi-GB load.
_BULK_COMMIT_CHUNKS = 32


def _pg(conn: duckdb.DuckDBPyConnection, sql: str) -> None:
    conn.execute(f"CALL postgres_execute('pg_target', $${sql}$$)")


def _pg_has_history(conn: duckdb.DuckDBPyConnection) -> bool:
    """True if the PG target already holds the history layer (seq_id=0); checked cheaply with LIMIT 1."""
    probe = "SELECT count(*) FROM (SELECT 1 FROM pg_target.changeset_stats WHERE seq_id = 0 LIMIT 1) t"
    row = conn.execute(probe).fetchone()
    return bool(row and row[0])


def _push_changesets(conn: duckdb.DuckDBPyConnection, where: str = "") -> None:
    # Newer non-NULL wins, NULL never downgrades (mirrors the DuckDB-side merge).
    conn.execute(
        f"""
        INSERT INTO pg_target.changesets AS c (changeset_id, uid, created_at, hashtags, editor, geom)
        SELECT changeset_id, uid, created_at, hashtags, editor, geom FROM changesets {where}
        ON CONFLICT (changeset_id) DO UPDATE SET
            created_at = COALESCE(EXCLUDED.created_at, c.created_at),
            hashtags   = COALESCE(EXCLUDED.hashtags,   c.hashtags),
            editor     = COALESCE(EXCLUDED.editor,     c.editor),
            geom       = COALESCE(EXCLUDED.geom,       c.geom)
        """
    )


def _push_changeset_stats(conn: duckdb.DuckDBPyConnection, where: str = "") -> None:
    conn.execute(f"INSERT INTO pg_target.changeset_stats SELECT * FROM changeset_stats {where} ON CONFLICT DO NOTHING")


def _push_chunked(conn: duckdb.DuckDBPyConnection, source: str, push) -> None:
    """Call push() once per changeset_id range so each range commits on its own."""
    bounds = conn.execute(f"SELECT min(changeset_id), max(changeset_id) FROM {source}").fetchone()
    if not bounds or bounds[0] is None:
        return
    lo, hi = bounds
    step = (hi - lo) // _BULK_COMMIT_CHUNKS + 1
    cursor = lo
    while cursor <= hi:
        push(conn, f"WHERE changeset_id >= {cursor} AND changeset_id < {cursor + step}")
        cursor += step


def to_psql(conn: duckdb.DuckDBPyConnection, dsn: str, *, bulk_load: bool = False) -> None:
    """Push every osmsg table to the libpq DSN target. bulk_load is for the one-time full-history
    import (drops indexes and foreign keys, streams, rebuilds, commits per range); leave it off for
    incremental --update pushes. The DSN is interpolated into ATTACH, so it must be trusted."""
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
                _pg(conn, stmt)

        # Refuse cross-source push: would double-count via the (seq_id, changeset_id) PK.
        local_sources = {r[0] for r in conn.execute("SELECT source_url FROM state").fetchall()}
        existing_sources = {r[0] for r in conn.execute("SELECT source_url FROM pg_target.state").fetchall()}
        cross_source = existing_sources - local_sources
        if cross_source and local_sources:
            raise OsmsgError(
                f"PG target already has data from source(s) {sorted(cross_source)} "
                f"but this run pushes from {sorted(local_sources)}. Mixing sources "
                f"double-counts via the (seq_id, changeset_id) key. Use a separate "
                f"--psql-dsn, or wipe the existing PG tables first."
            )

        if bulk_load:
            # Stream rows instead of buffering them to preserve order; buffering 180M+ JSON-bearing
            # rows is what exhausts memory in a single INSERT. Then drop the secondary indexes and
            # foreign keys so the load does not maintain them per row.
            conn.execute("SET preserve_insertion_order = false")
            for table, name, _add in _BULK_FKS:
                _pg(conn, f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")
            for name, _create in _BULK_INDEXES:
                _pg(conn, f"DROP INDEX IF EXISTS {name}")
            conn.execute("INSERT INTO pg_target.users SELECT * FROM users ON CONFLICT DO NOTHING")
            _push_chunked(conn, "changesets", _push_changesets)
            _push_chunked(conn, "changeset_stats", _push_changeset_stats)
        elif _pg_has_history(conn):
            # The history layer (seq_id=0) is already in PG from the bulk load and never changes, so an
            # incremental --update pushes only the live layer and its parents, not the 180M history rows.
            live_ids = "changeset_id IN (SELECT changeset_id FROM changeset_stats WHERE seq_id <> 0)"
            conn.execute(
                "INSERT INTO pg_target.users SELECT * FROM users "
                "WHERE uid IN (SELECT uid FROM changeset_stats WHERE seq_id <> 0) ON CONFLICT DO NOTHING"
            )
            _push_changesets(conn, f"WHERE {live_ids}")
            _push_changeset_stats(conn, "WHERE seq_id <> 0")
        else:
            # No history in PG (a plain live target): push everything (live rows are all seq_id<>0).
            conn.execute("INSERT INTO pg_target.users SELECT * FROM users ON CONFLICT DO NOTHING")
            _push_changesets(conn)
            _push_changeset_stats(conn)

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

        if bulk_load:
            # Rebuild once, with more memory for the sort-based index builds, then refresh planner stats.
            for table, name, add in _BULK_FKS:
                _pg(conn, f"ALTER TABLE {table} ADD CONSTRAINT {name} {add}")
            for _name, create in _BULK_INDEXES:
                _pg(conn, f"SET maintenance_work_mem = '512MB'; {create}")
            _pg(conn, "ANALYZE users")
            _pg(conn, "ANALYZE changesets")
            _pg(conn, "ANALYZE changeset_stats")
    finally:
        conn.execute("DETACH pg_target")


__all__ = ["PG_SCHEMA", "to_psql"]
