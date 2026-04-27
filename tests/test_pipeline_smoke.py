"""Smoke tests for the orchestration glue (no network)."""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest
from platformdirs import user_cache_dir

from osmsg.db.schema import create_tables, upsert_state
from osmsg.exceptions import OsmsgError
from osmsg.pipeline import RunConfig, _canonical_hashtags, _normalize_urls, _resolve_url_starts


def test_normalize_urls_expands_minute_shortcut():
    cfg = RunConfig(urls=["minute"])
    _normalize_urls(cfg)
    assert cfg.urls == ["https://planet.openstreetmap.org/replication/minute"]


def test_normalize_urls_strips_trailing_slash():
    cfg = RunConfig(urls=["https://example.com/path/"])
    _normalize_urls(cfg)
    assert cfg.urls == ["https://example.com/path"]


def test_normalize_urls_dedupes():
    cfg = RunConfig(urls=["minute", "minute"])
    _normalize_urls(cfg)
    assert len(cfg.urls) == 1


def test_normalize_urls_preserves_order():
    """Order matters: cfg.urls[0] used to be the implicit resume key. Set comprehension was non-deterministic."""
    cfg = RunConfig(urls=["https://example.com/zebra", "https://example.com/alpha", "https://example.com/zebra"])
    _normalize_urls(cfg)
    assert cfg.urls == ["https://example.com/zebra", "https://example.com/alpha"]


def test_run_config_defaults_to_parquet():
    cfg = RunConfig()
    assert cfg.formats == ["parquet"]
    # Library callers get the same per-user cache dir as the CLI, not a CWD-relative path.
    assert str(cfg.cache_dir) == user_cache_dir("osmsg")


def test_canonical_hashtags_normalizes_prefix():
    """Both 'hotosm' and '#hotosm' must canonicalize to the same '#hotosm' form (regression N1)."""
    assert _canonical_hashtags(["hotosm"]) == ["#hotosm"]
    assert _canonical_hashtags(["#hotosm"]) == ["#hotosm"]
    assert _canonical_hashtags(["##hotosm"]) == ["#hotosm"]
    assert _canonical_hashtags(["mapathon", "#hotosm-project-1"]) == ["#mapathon", "#hotosm-project-1"]


def _open_db(tmp_path):
    conn = duckdb.connect(str(tmp_path / "t.duckdb"))
    create_tables(conn)
    return conn


def test_resolve_url_starts_no_update_uses_cfg_start(tmp_path):
    conn = _open_db(tmp_path)
    start = dt.datetime(2026, 4, 1, tzinfo=dt.UTC)
    cfg = RunConfig(urls=["https://x", "https://y"], start_date=start)
    starts = _resolve_url_starts(conn, cfg)
    assert starts == {"https://x": start, "https://y": start}


def test_resolve_url_starts_no_update_no_start_raises(tmp_path):
    conn = _open_db(tmp_path)
    cfg = RunConfig(urls=["https://x"])
    with pytest.raises(OsmsgError, match="start_date is required"):
        _resolve_url_starts(conn, cfg)


def test_resolve_url_starts_update_reads_each_url_state_row(tmp_path):
    """Multi-URL --update must derive each URL's start from its own state row (regression N2/T1)."""
    conn = _open_db(tmp_path)
    ts_x = dt.datetime(2026, 4, 25, 12, 0, tzinfo=dt.UTC)
    ts_y = dt.datetime(2026, 4, 26, 9, 30, tzinfo=dt.UTC)
    upsert_state(conn, source_url="https://x", last_seq=1, last_ts=ts_x, updated_at=ts_x)
    upsert_state(conn, source_url="https://y", last_seq=2, last_ts=ts_y, updated_at=ts_y)
    cfg = RunConfig(urls=["https://x", "https://y"], update=True)
    starts = _resolve_url_starts(conn, cfg)
    assert starts == {"https://x": ts_x, "https://y": ts_y}


def test_resolve_url_starts_update_missing_state_raises_per_url(tmp_path):
    conn = _open_db(tmp_path)
    upsert_state(
        conn,
        source_url="https://x",
        last_seq=1,
        last_ts=dt.datetime(2026, 4, 25, tzinfo=dt.UTC),
        updated_at=dt.datetime(2026, 4, 25, tzinfo=dt.UTC),
    )
    cfg = RunConfig(urls=["https://x", "https://y"], update=True)
    with pytest.raises(OsmsgError, match="--update has no prior state for https://y"):
        _resolve_url_starts(conn, cfg)
