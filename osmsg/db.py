from __future__ import annotations
import json
import duckdb
from . import models
import threading

BATCH_SIZE = 2000


# connection
def get_connection(db_path: str = "scratch.db") -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(db_path)
    try:
        conn.execute("INSTALL spatial")
    except duckdb.IOException:
        pass  # already installed — safe to ignore

    # LOAD makes the extension's functions available for this connection.
    conn.execute("LOAD spatial")

    return conn


# Table Creation
def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    # TABLE 1: users
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            uid      BIGINT PRIMARY KEY,
            username VARCHAR NOT NULL
        )
    """)

    # TABLE 2: changesets
    conn.execute("""
        CREATE TABLE IF NOT EXISTS changesets (
            changeset_id BIGINT  PRIMARY KEY,
            uid          BIGINT NOT NULL REFERENCES users(uid),
            created_at   TIMESTAMPTZ,
            hashtags     VARCHAR[],
            editor       VARCHAR,
            bbox         GEOMETRY
        )
    """)

    # Index on created_at
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_changesets_created_at
        ON changesets (created_at)
    """)

    # TABLE 3: changeset_stats
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS changeset_stats (
            changeset_id  BIGINT  PRIMARY KEY REFERENCES changesets(changeset_id),
            uid           BIGINT NOT NULL REFERENCES users(uid),
 
            nodes_created   INTEGER DEFAULT 0,
            nodes_modified  INTEGER DEFAULT 0,
            nodes_deleted   INTEGER DEFAULT 0,
 
            ways_created    INTEGER DEFAULT 0,
            ways_modified   INTEGER DEFAULT 0,
            ways_deleted    INTEGER DEFAULT 0,
 
            rels_created    INTEGER DEFAULT 0,
            rels_modified   INTEGER DEFAULT 0,
            rels_deleted    INTEGER DEFAULT 0,
 
            poi_created     INTEGER DEFAULT 0,
            poi_modified    INTEGER DEFAULT 0,
 
            tag_stats       JSON
        )
    """)

    # Index on uid
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_changeset_stats_uid
        ON changeset_stats (uid)
    """)


# insert functions
def insert_users(
    conn: duckdb.DuckDBPyConnection,
    rows: list[tuple[int, str]],
) -> None:
    """
    Batch-insert users into the 'users' table
    """
    if not rows:
        return
    conn.executemany(
        "INSERT OR IGNORE INTO users VALUES (?, ?)",
        rows,
    )


def insert_changesets(
    conn: duckdb.DuckDBPyConnection,
    rows: list[tuple],
) -> None:
    """
    Batch-insert changesets into the 'changesets' table.

    Parameters
    ----------
    rows : list of tuples in this exact order:
        (
            changeset_id : int,
            uid          : int,
            created_at   : datetime | None,
            hashtags     : list[str] | None,
            editor       : str | None,
            bbox_wkt     : str | None,   ← WKT string from bbox_to_wkt(),
        )                                   or None if no bbox was in the file

    Callers should use prepare_changeset_row() below to build the tuple
    safely.
    """
    if not rows:
        return
    conn.executemany(
        """
        INSERT OR IGNORE INTO changesets
            (changeset_id, uid, created_at, hashtags, editor, bbox)
        VALUES
            (?, ?, ?, ?, ?, ST_SetCRS(?::GEOMETRY, 'EPSG:4326'))
        """,
        rows,
    )


def insert_changeset_stats(
    conn: duckdb.DuckDBPyConnection,
    rows: list[tuple],
) -> None:
    """
    Batch-insert changeset stats into the 'changeset_stats' table.

    Parameters
    ----------
    rows : list of tuples in this exact order:
        (
            changeset_id  : int,
            uid           : int,
            nodes_created : int,
            nodes_modified: int,
            nodes_deleted : int,
            ways_created  : int,
            ways_modified : int,
            ways_deleted  : int,
            rels_created  : int,
            rels_modified : int,
            rels_deleted  : int,
            poi_created   : int,
            poi_modified  : int,
            tag_stats_json: str,  ← json.dumps(stats.tag_stats_as_dict())
        )

    Callers should use prepare_stats_row() below to build the tuple safely.
    """
    if not rows:
        return
    conn.executemany(
        "INSERT OR IGNORE INTO changeset_stats VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )


# helper function
def bbox_to_wkt(bbox: tuple[float, float, float, float]) -> str:
    min_lon, min_lat, max_lon, max_lat = bbox
    return (
        f"POLYGON(("
        f"{min_lon} {min_lat}, "  # SW
        f"{max_lon} {min_lat}, "  # SE
        f"{max_lon} {max_lat}, "  # NE
        f"{min_lon} {max_lat}, "  # NW
        f"{min_lon} {min_lat}"  # SW
        f"))"
    )


# row preparation helpers
def prepare_changeset_row(changeset: models.Changeset) -> tuple:
    """
    Convert a Changeset Pydantic object to the tuple format expected by
    insert_changesets().
    """
    bbox_wkt = bbox_to_wkt(changeset.bbox) if changeset.bbox is not None else None
    return (
        changeset.changeset_id,
        changeset.uid,
        changeset.created_at,
        changeset.hashtags if changeset.hashtags else None,
        changeset.editor,
        bbox_wkt,
    )


def prepare_stats_row(stats: models.ChangesetStats) -> tuple:
    """
    Convert a ChangesetStats Pydantic object to the tuple format expected by insert_changeset_stats().
    """
    flat = stats.to_dict
    return (
        stats.changeset_id,
        stats.uid,
        flat["nodes_created"],
        flat["nodes_modified"],
        flat["nodes_deleted"],
        flat["ways_created"],
        flat["ways_modified"],
        flat["ways_deleted"],
        flat["rels_created"],
        flat["rels_modified"],
        flat["rels_deleted"],
        flat["poi_created"],
        flat["poi_modified"],
        json.dumps(flat["tag_stats"]),
    )
