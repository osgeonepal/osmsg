"""Round-trip golden tests for parquet/csv/json/markdown writers."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pyarrow.parquet as pq

from osmsg.export.csv import to_csv
from osmsg.export.json import to_json
from osmsg.export.markdown import summary_markdown, table_markdown
from osmsg.export.parquet import to_parquet

SAMPLE_ROWS = [
    {
        "rank": 1,
        "uid": 10,
        "name": "alice",
        "changesets": 5,
        "map_changes": 100,
        "tags_create": {"building": 5},
        "hashtags": ["#hotosm"],
        "editors": ["iD"],
    },
    {
        "rank": 2,
        "uid": 20,
        "name": "bob",
        "changesets": 1,
        "map_changes": 5,
        "tags_create": {},
        "hashtags": [],
        "editors": [],
    },
]


def test_parquet_round_trip(tmp_path: Path):
    out = to_parquet(SAMPLE_ROWS, tmp_path / "stats.parquet")
    table = pq.read_table(out)
    by_name = {row["name"]: row for row in table.to_pylist()}
    assert by_name["alice"]["map_changes"] == 100
    # nested dicts JSON-encoded for parquet portability
    assert json.loads(by_name["alice"]["tags_create"]) == {"building": 5}


def test_csv_round_trip_lists_joined(tmp_path: Path):
    out = to_csv(SAMPLE_ROWS, tmp_path / "stats.csv")
    rows = list(csv.DictReader(out.open()))
    by_name = {r["name"]: r for r in rows}
    assert by_name["alice"]["map_changes"] == "100"
    assert by_name["alice"]["hashtags"] == "#hotosm"


def test_json_writes_native_types(tmp_path: Path):
    out = to_json(SAMPLE_ROWS, tmp_path / "stats.json")
    payload = json.loads(out.read_text())
    assert payload[0]["hashtags"] == ["#hotosm"]
    assert payload[0]["tags_create"] == {"building": 5}


def test_table_markdown_renders_header_and_rows():
    md = table_markdown(SAMPLE_ROWS, headers=["rank", "name", "map_changes"])
    lines = md.splitlines()
    assert lines[0] == "| rank | name | map_changes |"
    assert lines[1] == "| --- | --- | --- |"
    assert "alice" in lines[2]


def test_summary_markdown_writes_top_users_and_totals(tmp_path: Path):
    out = summary_markdown(
        SAMPLE_ROWS
        + [
            {
                "name": "alice",
                "nodes_create": 10,
                "ways_create": 5,
                "rels_create": 0,
                "nodes_modify": 0,
                "ways_modify": 0,
                "rels_modify": 0,
                "nodes_delete": 0,
                "ways_delete": 0,
                "rels_delete": 0,
                "poi_create": 5,
                "poi_modify": 1,
                "changesets": 1,
                "map_changes": 15,
            }
        ],
        output_path=tmp_path / "stats_summary.md",
        start_date="2026-04-01",
        end_date="2026-04-02",
        all_tags=True,
        fname="stats",
    )
    body = out.read_text()
    assert "Top 5 users" in body
    assert "alice" in body
    assert "Stats from 2026-04-01 to 2026-04-02" in body
