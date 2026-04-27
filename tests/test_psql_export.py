"""Schema validation for the PostgreSQL exporter.

A live push test requires a Postgres instance and is gated behind the
`OSMSG_PG_DSN` env var (mark `network` to deselect by default).
"""

from __future__ import annotations

import os
import re

import duckdb
import pytest

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
    duckdb_clone = PG_SCHEMA.replace("DOUBLE PRECISION", "DOUBLE").replace("JSONB", "JSON").replace("TEXT", "VARCHAR")
    conn = duckdb.connect(":memory:")
    for stmt in [s.strip() for s in duckdb_clone.split(";") if s.strip()]:
        conn.execute(stmt)
    tables = {r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    assert {"users", "changesets", "changeset_stats", "state"} <= tables


@pytest.mark.network
@pytest.mark.skipif(not os.environ.get("OSMSG_PG_DSN"), reason="OSMSG_PG_DSN not set; live PG push not exercised")
def test_live_push_to_postgres(fresh_db, populated_db_factory):
    """Live test: push a populated DuckDB into the PG instance specified by OSMSG_PG_DSN.

    Pre-requisite — a reachable PG with rights to create tables, e.g.:
        export OSMSG_PG_DSN="host=localhost port=5432 dbname=osmsg_test user=osm password=osm"
    """
    populated = populated_db_factory(fresh_db)
    to_psql(populated, os.environ["OSMSG_PG_DSN"])
