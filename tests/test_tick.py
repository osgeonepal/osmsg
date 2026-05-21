"""Worker tick: command assembly + state-row lookup precedence."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pytest

from osmsg import _tick
from osmsg.db import connect, create_tables
from osmsg.db.schema import upsert_state
from osmsg.geofabrik import country_update_url
from osmsg.replication import SHORTCUTS


@pytest.fixture
def captured_cmd(monkeypatch):
    captured: dict[str, Any] = {}

    def fake_call(cmd, *args, **kwargs):
        captured["cmd"] = list(cmd)
        return 0

    monkeypatch.setattr(_tick.subprocess, "call", fake_call)
    return captured


@pytest.fixture
def clean_env(monkeypatch):
    for key in ("OSMSG_EXTRA_ARGS", "OSMSG_BOOTSTRAP", "OSMSG_BOOTSTRAP_DAYS"):
        monkeypatch.delenv(key, raising=False)


def _seed_state(out_dir: Path, name: str, source_url: str) -> None:
    conn = connect(str(out_dir / f"{name}.duckdb"))
    try:
        create_tables(conn)
        ts = dt.datetime(2026, 5, 21, 7, 0, tzinfo=dt.UTC)
        upsert_state(conn, source_url=source_url, last_seq=100, last_ts=ts, updated_at=ts)
    finally:
        conn.close()


def test_explicit_url_with_country_resolves_state_under_explicit_url(tmp_path, monkeypatch, captured_cmd, clean_env):
    """--country + explicit --url: state row is keyed by the explicit URL (pipeline rule).

    Regression guard: previously _tick looked up state under the country's geofabrik URL,
    never found it, and re-bootstrapped every tick (wiping the DuckDB each time).
    """
    name = "nepal"
    _seed_state(tmp_path, name, SHORTCUTS["minute"])

    monkeypatch.setenv(
        "OSMSG_EXTRA_ARGS",
        f"--name {name} --output-dir {tmp_path} --country nepal --url minute",
    )

    assert _tick.main() == 0
    assert "--update" in captured_cmd["cmd"], (
        f"expected --update to be appended when state exists for the explicit URL; got {captured_cmd['cmd']}"
    )
    assert "--last" not in captured_cmd["cmd"]


def test_country_only_resolves_state_under_geofabrik_url(tmp_path, monkeypatch, captured_cmd, clean_env):
    """--country alone: state is keyed by geofabrik (pipeline derives URL from country)."""
    name = "nepal"
    _seed_state(tmp_path, name, country_update_url("nepal"))

    monkeypatch.setenv(
        "OSMSG_EXTRA_ARGS",
        f"--name {name} --output-dir {tmp_path} --country nepal",
    )

    assert _tick.main() == 0
    assert "--update" in captured_cmd["cmd"]


def test_no_state_appends_bootstrap_window(tmp_path, monkeypatch, captured_cmd, clean_env):
    """First tick (no state row) → --last <bootstrap> instead of --update."""
    name = "nepal"
    monkeypatch.setenv("OSMSG_EXTRA_ARGS", f"--name {name} --output-dir {tmp_path} --url minute")
    monkeypatch.setenv("OSMSG_BOOTSTRAP", "hour")

    assert _tick.main() == 0
    cmd = captured_cmd["cmd"]
    assert "--update" not in cmd
    assert cmd[-2:] == ["--last", "hour"]


def test_bootstrap_days_overrides_bootstrap_preset(tmp_path, monkeypatch, captured_cmd, clean_env):
    name = "nepal"
    monkeypatch.setenv("OSMSG_EXTRA_ARGS", f"--name {name} --output-dir {tmp_path}")
    monkeypatch.setenv("OSMSG_BOOTSTRAP_DAYS", "3")

    assert _tick.main() == 0
    cmd = captured_cmd["cmd"]
    assert cmd[-2:] == ["--days", "3"]
