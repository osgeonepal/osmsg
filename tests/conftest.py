"""Shared fixtures: synthetic osmium changefiles + DuckDB test DB."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import osmium
import pytest

from osmsg.db.schema import create_tables


def _writer(path: Path) -> osmium.SimpleWriter:
    return osmium.SimpleWriter(str(path), overwrite=True)


@pytest.fixture
def osc_factory(tmp_path: Path):
    """Build a tiny .osc file. Caller passes a list of (kind, kwargs)."""

    def _build(name: str, items: list[tuple[str, dict]]) -> Path:
        path = tmp_path / name
        w = _writer(path)
        try:
            for kind, kwargs in items:
                if kind == "node":
                    w.add_node(osmium.osm.mutable.Node(**kwargs))
                elif kind == "way":
                    w.add_way(osmium.osm.mutable.Way(**kwargs))
                elif kind == "relation":
                    w.add_relation(osmium.osm.mutable.Relation(**kwargs))
                else:
                    raise ValueError(kind)
        finally:
            w.close()
        return path

    return _build


@pytest.fixture
def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


@pytest.fixture
def fresh_db(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    create_tables(conn)
    yield conn
    conn.close()


@pytest.fixture
def populated_db_factory():
    """Returns a callable that fills a fresh DuckDB with a small, fixed dataset.

    Used by tests that need realistic data without rebuilding it inline.
    """

    def _populate(conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
        conn.execute("INSERT INTO users VALUES (10, 'alice'), (20, 'bob')")
        conn.execute(
            """
            INSERT INTO changesets
            VALUES (1, 10, '2026-04-01 10:00:00+00', ['#mapathon'], 'iD', 85.0, 27.0, 85.5, 27.5),
                   (2, 20, '2026-04-02 09:00:00+00', NULL, 'JOSM', NULL, NULL, NULL, NULL)
            """
        )
        conn.execute(
            """
            INSERT INTO changeset_stats VALUES
                (1, 100, 10, 30, 5, 0, 8, 1, 0, 0, 0, 0, 5, 1, NULL),
                (2, 101, 20, 50, 0, 0, 0, 0, 0, 0, 0, 0, 50, 0, NULL)
            """
        )
        return conn

    return _populate


@pytest.fixture
def changefile_config():
    """Default config with a wide window so synthetic .osc files (1970-default ts) are kept."""
    return {
        "hashtags": None,
        "additional_tags": ["building", "highway"],
        "all_tags": False,
        "key_value": False,
        "length": None,
        "exact_lookup": False,
        "changeset_meta": False,
        "whitelisted_users": [],
        "geom_filter_wkt": None,
        "delete_temp": False,
        "cache_dir": "temp",
        "parquet_dir": "temp_parquet",
        "start_date_utc": dt.datetime(1969, 1, 1, tzinfo=dt.UTC),
        "end_date_utc": dt.datetime(2099, 12, 31, tzinfo=dt.UTC),
    }
