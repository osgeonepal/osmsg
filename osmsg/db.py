from __future__ import annotations
import glob
import json
import os
import duckdb
from . import models
import shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


def flush_rows_to_parquet(
    rows_users: list[tuple],
    rows_changesets: list[tuple],
    rows_stats: list[tuple] | None,
    pid: int,
    batch_index: int,
    parquet_dir: str,
) -> tuple[str | None, str | None, str | None]:
    """
    Write one batch of in-memory rows list to per-worker Parquet files.

    Each call appends a new file named
      <parquet_dir>/temp_<pid>_users_<batch_index>.parquet
      <parquet_dir>/temp_<pid>_changesets_<batch_index>.parquet
      <parquet_dir>/temp_<pid>_stats_<batch_index>.parquet   (when rows_stats is not None)

    Returns the three file paths (or None when the list empty).
    """

    def _write(data, schema, path):
        if not data:
            return None
        # convert list of rows to list of columns
        data_columnar = list(zip(*data))

        arrays = [pa.array(col, type=field.type) for col, field in zip(data_columnar, schema, strict=True)]
        table = pa.table(dict(zip(schema.names, arrays)))
        pq.write_table(table, path, compression="snappy")
        return path

    user_schema = pa.schema(
        [
            pa.field("uid", pa.int64(), nullable=False),
            pa.field("username", pa.string(), nullable=False),
        ]
    )

    cs_schema = pa.schema(
        [
            pa.field("changeset_id", pa.int64(), nullable=False),
            pa.field("uid", pa.int64(), nullable=False),
            pa.field("created_at", pa.timestamp("s", tz="UTC")),
            pa.field("hashtags", pa.list_(pa.string())),
            pa.field("editor", pa.string()),
            pa.field("bbox_wkt", pa.string()),
        ]
    )

    stats_schema = pa.schema(
        [
            pa.field("changeset_id", pa.int64(), nullable=False),
            pa.field("seq_id", pa.int64()),
            pa.field("uid", pa.int64()),
            pa.field("nodes_created", pa.int32()),
            pa.field("nodes_modified", pa.int32()),
            pa.field("nodes_deleted", pa.int32()),
            pa.field("ways_created", pa.int32()),
            pa.field("ways_modified", pa.int32()),
            pa.field("ways_deleted", pa.int32()),
            pa.field("rels_created", pa.int32()),
            pa.field("rels_modified", pa.int32()),
            pa.field("rels_deleted", pa.int32()),
            pa.field("poi_created", pa.int32()),
            pa.field("poi_modified", pa.int32()),
            pa.field("tag_stats", pa.string()),
        ]
    )

    u_path = _write(rows_users, user_schema, os.path.join(parquet_dir, f"temp_{pid}_users_{batch_index}.parquet"))
    cs_path = _write(rows_changesets, cs_schema, os.path.join(parquet_dir, f"temp_{pid}_changesets_{batch_index}.parquet"))
    st_path = (
        _write(rows_stats, stats_schema, os.path.join(parquet_dir, f"temp_{pid}_stats_{batch_index}.parquet"))
        if rows_stats is not None
        else None
    )

    return u_path, cs_path, st_path


def merge_parquet_files(
    conn: duckdb.DuckDBPyConnection,
    parquet_dir: str,
    cleanup: bool = True,
) -> None:
    """
    Merge all per-worker Parquet files into the main DuckDB tables in a single vectorised pass per table.  DuckDB parallelises the read_parquet() scan internally.

    After merging, the Parquet files are removed (unless cleanup=False).
    """
    path = Path(parquet_dir)

    user_pattern = Path(parquet_dir, "temp_*_users_*.parquet").as_posix()  # forward slashes instead of black slashes
    cs_pattern = Path(parquet_dir, "temp_*_changesets_*.parquet").as_posix()
    stats_pattern = Path(parquet_dir, "temp_*_stats_*.parquet").as_posix()

    conn.execute("BEGIN")

    try:
        if any(path.glob("temp_*_users_*.parquet")):
            conn.execute(
                f"""
                INSERT OR IGNORE INTO users
                SELECT uid, username
                FROM read_parquet('{user_pattern}')
                """
            )

        # changesets (WKT → GEOMETRY conversion happens here)
        if any(path.glob("temp_*_changesets_*.parquet")):
            conn.execute(
                f"""
                INSERT OR IGNORE INTO changesets
                SELECT
                    changeset_id,
                    uid,
                    created_at,
                    hashtags,
                    editor,
                    CASE
                        WHEN bbox_wkt IS NOT NULL
                        THEN ST_SetCRS(bbox_wkt::GEOMETRY, 'EPSG:4326')
                        ELSE NULL
                    END AS bbox
                FROM read_parquet('{cs_pattern}')
                """
            )

        # changeset_stats
        if any(path.glob("temp_*_stats_*.parquet")):
            conn.execute(
                f"""
                INSERT OR IGNORE INTO changeset_stats
                SELECT
                    changeset_id, seq_id, uid,
                    nodes_created, nodes_modified, nodes_deleted,
                    ways_created,  ways_modified,  ways_deleted,
                    rels_created,  rels_modified,  rels_deleted,
                    poi_created,   poi_modified,
                    tag_stats::JSON AS tag_stats
                FROM read_parquet('{stats_pattern}')
                """
            )

        conn.execute("COMMIT")

        if cleanup:
            shutil.rmtree(parquet_dir, ignore_errors=True)

    except Exception:
        conn.execute("ROLLBACK")
        raise


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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            uid      BIGINT PRIMARY KEY,
            username VARCHAR NOT NULL
        )
    """
    )

    # TABLE 2: changesets
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS changesets (
            changeset_id BIGINT  PRIMARY KEY,
            uid          BIGINT NOT NULL REFERENCES users(uid),
            created_at   TIMESTAMPTZ,
            hashtags     VARCHAR[],
            editor       VARCHAR,
            bbox         GEOMETRY
        )
    """
    )

    # Index on created_at
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_changesets_created_at
        ON changesets (created_at)
    """
    )

    # TABLE 3: changeset_stats
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS changeset_stats (
            changeset_id  BIGINT   REFERENCES changesets(changeset_id),
            seq_id        BIGINT NOT NULL,
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
 
            tag_stats       JSON,
                 
            -- Composite Primary Key
            PRIMARY KEY (seq_id, changeset_id)
        )
    """
    )

    # Index on uid
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_changeset_stats_uid
        ON changeset_stats (uid)
    """
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
    return (
        stats.changeset_id,
        stats.seq_id,
        stats.uid,
        stats.nodes.c,
        stats.nodes.m,
        stats.nodes.d,
        stats.ways.c,
        stats.ways.m,
        stats.ways.d,
        stats.rels.c,
        stats.rels.m,
        stats.rels.d,
        stats.poi_created,
        stats.poi_modified,
        json.dumps(stats.tag_stats_as_dict),
    )
