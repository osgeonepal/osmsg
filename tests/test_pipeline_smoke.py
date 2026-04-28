"""Smoke tests for the orchestration glue (no network)."""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest
from platformdirs import user_cache_dir

from osmsg.db.schema import create_tables, upsert_state
from osmsg.exceptions import OsmsgError
from osmsg.pipeline import (
    RunConfig,
    _auto_switch_replication,
    _canonical_hashtags,
    _normalize_urls,
    _pick_replication_for_span,
    _resolve_url_starts,
)
from osmsg.replication import SHORTCUTS


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
    with pytest.raises(OsmsgError, match="no prior state for https://y"):
        _resolve_url_starts(conn, cfg)


def test_resolve_url_starts_update_error_lists_known_urls_and_invariant(tmp_path):
    """The error must surface (a) which URLs are seeded and (b) the seq_id double-count rationale —
    so the user knows their two recovery options without spelunking the source."""
    conn = _open_db(tmp_path)
    upsert_state(
        conn,
        source_url="https://planet.openstreetmap.org/replication/minute",
        last_seq=1,
        last_ts=dt.datetime(2026, 4, 25, tzinfo=dt.UTC),
        updated_at=dt.datetime(2026, 4, 25, tzinfo=dt.UTC),
    )
    cfg = RunConfig(urls=["https://planet.openstreetmap.org/replication/day"], update=True)
    with pytest.raises(OsmsgError) as exc:
        _resolve_url_starts(conn, cfg)
    msg = str(exc.value)
    assert "Existing state in this DuckDB is for" in msg
    assert "minute" in msg  # known URL surfaced
    assert "different --name" in msg  # recovery hint
    assert "seq_id" in msg  # invariant referenced


@pytest.mark.parametrize(
    "span,expected",
    [
        (dt.timedelta(hours=1), "minute"),
        (dt.timedelta(hours=5, minutes=59), "minute"),
        (dt.timedelta(hours=6), "hour"),  # boundary: ≥6h flips to hour
        (dt.timedelta(days=1), "hour"),
        (dt.timedelta(days=6, hours=23), "hour"),
        (dt.timedelta(days=7), "day"),  # boundary: ≥7d flips to day
        (dt.timedelta(days=30), "day"),
    ],
)
def test_pick_replication_for_span(span, expected):
    assert _pick_replication_for_span(span) == expected


def test_auto_switch_promotes_minute_to_hour_on_long_span(capsys):
    cfg = RunConfig(urls=[SHORTCUTS["minute"]])
    _auto_switch_replication(cfg, dt.timedelta(hours=10))
    assert cfg.urls == [SHORTCUTS["hour"]]
    err = capsys.readouterr().err
    assert "auto-switching" in err
    assert "from 'minute' to 'hour'" in err


def test_auto_switch_promotes_minute_to_day_on_multi_day_span():
    cfg = RunConfig(urls=[SHORTCUTS["minute"]])
    _auto_switch_replication(cfg, dt.timedelta(days=30))
    assert cfg.urls == [SHORTCUTS["day"]]


def test_auto_switch_demotes_day_to_minute_on_short_span():
    """A user defaulting to day for a 1h window should be moved back to minute too."""
    cfg = RunConfig(urls=[SHORTCUTS["day"]])
    _auto_switch_replication(cfg, dt.timedelta(hours=1))
    assert cfg.urls == [SHORTCUTS["minute"]]


def test_auto_switch_no_op_when_already_correct(capsys):
    cfg = RunConfig(urls=[SHORTCUTS["hour"]])
    _auto_switch_replication(cfg, dt.timedelta(hours=10))
    assert cfg.urls == [SHORTCUTS["hour"]]
    assert "auto-switching" not in capsys.readouterr().err


def test_auto_switch_suppressed_by_url_explicit():
    cfg = RunConfig(urls=[SHORTCUTS["minute"]], url_explicit=True)
    _auto_switch_replication(cfg, dt.timedelta(days=30))
    assert cfg.urls == [SHORTCUTS["minute"]]


def test_auto_switch_suppressed_by_update():
    """--update must never auto-switch — cross-URL replay would double-count via (seq_id, changeset_id)."""
    cfg = RunConfig(urls=[SHORTCUTS["minute"]], update=True)
    _auto_switch_replication(cfg, dt.timedelta(days=30))
    assert cfg.urls == [SHORTCUTS["minute"]]


def test_auto_switch_suppressed_by_country():
    cfg = RunConfig(urls=[SHORTCUTS["minute"]], countries=["nepal"])
    _auto_switch_replication(cfg, dt.timedelta(days=30))
    assert cfg.urls == [SHORTCUTS["minute"]]


def test_auto_switch_suppressed_by_multi_url():
    urls = [SHORTCUTS["minute"], SHORTCUTS["hour"]]
    cfg = RunConfig(urls=list(urls))
    _auto_switch_replication(cfg, dt.timedelta(days=30))
    assert cfg.urls == urls


def test_auto_switch_skips_non_shortcut_url():
    """A custom (e.g. Geofabrik) URL must not be silently swapped for a planet shortcut."""
    custom = "https://download.geofabrik.de/asia/nepal-updates"
    cfg = RunConfig(urls=[custom])
    _auto_switch_replication(cfg, dt.timedelta(days=30))
    assert cfg.urls == [custom]
