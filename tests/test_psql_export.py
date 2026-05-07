"""Schema validation for the PostgreSQL exporter.

A live push test requires a Postgres instance and is gated behind the
`OSMSG_PG_DSN` env var (mark `network` to deselect by default).
"""

from __future__ import annotations

import os
import re

import duckdb
import pyarrow.parquet as pq
import pytest

from osmsg.db.queries import user_stats
from osmsg.export.parquet import to_parquet
from osmsg.export.psql import PG_SCHEMA, to_psql


def test_pg_schema_contains_every_osmsg_table():
    """PG_SCHEMA must declare the full osmsg schema (no silent regression)."""
    statements = [s for s in PG_SCHEMA.strip().split(";") if s.strip()]
    table_names = {
        m.group(1)
        for s in statements
        for m in [re.search(r"CREATE TABLE IF NOT EXISTS\s+(\w+)", s, re.IGNORECASE)]
        if m
    }
    assert {"users", "changesets", "changeset_stats", "state"} <= table_names


def test_pg_schema_uses_jsonb_for_tag_stats():
    """JSONB (binary, indexable) is the right PG type for tag_stats."""
    assert "tag_stats      JSONB" in PG_SCHEMA


def test_pg_schema_state_is_single_row_per_source():
    """`state` is keyed by source_url alone — one row per replication source, ever.
    The PSQL exporter UPSERTs on conflict so every osmsg run keeps PG in sync."""
    assert "source_url  TEXT PRIMARY KEY" in PG_SCHEMA
    assert "BIGSERIAL" not in PG_SCHEMA  # no synthetic ids needed


def test_pg_schema_statements_each_parse_with_postgres_extension():
    """Each individual CREATE statement is well-formed enough that the postgres
    extension's parser would accept it — we use DuckDB's own parser as an
    approximation (DuckDB's CREATE TABLE syntax is compatible)."""
    duckdb_clone = (
        PG_SCHEMA.replace("DOUBLE PRECISION", "DOUBLE")
        .replace("JSONB", "JSON")
        .replace("TEXT", "VARCHAR")
        .replace("GEOMETRY(POLYGON)", "GEOMETRY")
    )
    conn = duckdb.connect(":memory:")
    conn.execute("LOAD spatial")
    for stmt in [s.strip() for s in duckdb_clone.split(";") if s.strip()]:
        upper = stmt.upper()
        if upper.startswith("CREATE EXTENSION") or "USING GIST" in upper:
            continue
        conn.execute(stmt)
    tables = {r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    assert {"users", "changesets", "changeset_stats", "state"} <= tables


EXPECTED_USER_STATS = {
    "alice": {"changesets": 1, "nodes_create": 30, "ways_create": 8, "poi_create": 5, "map_changes": 44},
    "bob": {"changesets": 1, "nodes_create": 50, "ways_create": 0, "poi_create": 50, "map_changes": 50},
}


def _assert_user_stats_match(actual: list[dict], expected: dict[str, dict[str, int]]) -> None:
    by_name = {r["name"]: r for r in actual}
    assert set(by_name) == set(expected), f"users mismatch: {set(by_name)} vs {set(expected)}"
    for name, fields in expected.items():
        for col, want in fields.items():
            assert by_name[name][col] == want, f"{name}.{col}: got {by_name[name][col]} want {want}"


def test_duckdb_user_stats_match_seed_data(fresh_db, populated_db_factory):
    """Anchors EXPECTED_USER_STATS against the seed fixture; if this drifts, every
    other roundtrip in this file silently compares against wrong numbers."""
    rows = user_stats(populated_db_factory(fresh_db))
    _assert_user_stats_match(rows, EXPECTED_USER_STATS)


def test_user_stats_roundtrip_through_parquet(tmp_path, fresh_db, populated_db_factory):
    rows = user_stats(populated_db_factory(fresh_db))
    out = to_parquet(rows, tmp_path / "stats.parquet")

    table = pq.read_table(out).to_pylist()
    _assert_user_stats_match(table, EXPECTED_USER_STATS)


@pytest.mark.network
@pytest.mark.skipif(not os.environ.get("OSMSG_PG_DSN"), reason="OSMSG_PG_DSN not set; live PG push not exercised")
def test_user_stats_roundtrip_through_postgres(fresh_db, populated_db_factory):
    populated = populated_db_factory(fresh_db)
    dsn = os.environ["OSMSG_PG_DSN"]

    populated.execute("INSTALL postgres")
    populated.execute("LOAD postgres")
    safe_dsn = dsn.replace("'", "''")
    populated.execute(f"ATTACH '{safe_dsn}' AS pg_wipe (TYPE postgres)")
    try:
        for stmt in PG_SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                populated.execute(f"CALL postgres_execute('pg_wipe', $${stmt}$$)")
        for table in ("changeset_stats", "changesets", "users", "state"):
            populated.execute(f"CALL postgres_execute('pg_wipe', $$DELETE FROM {table}$$)")
    finally:
        populated.execute("DETACH pg_wipe")

    to_psql(populated, dsn)

    verifier = duckdb.connect(":memory:")
    verifier.execute("INSTALL postgres")
    verifier.execute("LOAD postgres")
    verifier.execute(f"ATTACH '{safe_dsn}' AS pg_src (TYPE postgres, READ_ONLY)")
    try:
        rows = verifier.execute(
            """
            SELECT u.username AS name,
                   COUNT(DISTINCT cs.changeset_id) AS changesets,
                   SUM(cs.nodes_created) AS nodes_create,
                   SUM(cs.ways_created)  AS ways_create,
                   SUM(cs.poi_created)   AS poi_create,
                   SUM(
                       cs.nodes_created + cs.nodes_modified + cs.nodes_deleted +
                       cs.ways_created  + cs.ways_modified  + cs.ways_deleted  +
                       cs.rels_created  + cs.rels_modified  + cs.rels_deleted
                   ) AS map_changes
            FROM pg_src.users u
            JOIN pg_src.changeset_stats cs ON u.uid = cs.uid
            GROUP BY u.username
            """
        ).fetchall()
    finally:
        verifier.execute("DETACH pg_src")
        verifier.close()

    cols = ("name", "changesets", "nodes_create", "ways_create", "poi_create", "map_changes")
    actual = [dict(zip(cols, r, strict=True)) for r in rows]
    _assert_user_stats_match(actual, EXPECTED_USER_STATS)
