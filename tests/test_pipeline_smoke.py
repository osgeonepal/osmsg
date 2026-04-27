"""Smoke tests for the orchestration glue (no network)."""

from __future__ import annotations

from platformdirs import user_cache_dir

from osmsg.pipeline import RunConfig, _normalize_urls


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


def test_run_config_defaults_to_parquet():
    cfg = RunConfig()
    assert cfg.formats == ["parquet"]
    # Library callers get the same per-user cache dir as the CLI, not a CWD-relative path.
    assert str(cfg.cache_dir) == user_cache_dir("osmsg")
