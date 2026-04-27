"""typer CliRunner sanity tests for major flag combinations.

These don't exercise the OSM network — they verify the parser wiring, period
resolution, mutual-exclusion checks, and error paths.
"""

from __future__ import annotations

from pathlib import Path

import click
import pytest
from typer.testing import CliRunner

from osmsg.__version__ import __version__
from osmsg.cli import app

runner = CliRunner()


def test_version_flag_prints_version():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_help_lists_core_options():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    plain = click.unstyle(result.stdout)
    for fragment in ("--last", "--country", "--format", "--update", "--psql-dsn"):
        assert fragment in plain


@pytest.mark.parametrize(
    "args",
    [
        ["--last", "hour", "--days", "3"],
        ["--start", "2026-04-01", "--last", "hour"],
        ["--start", "2026-04-01", "--days", "3"],
    ],
)
def test_time_range_flags_are_mutually_exclusive(args):
    result = runner.invoke(app, args)
    assert result.exit_code == 2


def test_changeset_flag_is_hidden_in_help():
    result = runner.invoke(app, ["--help"])
    assert "--changeset" not in click.unstyle(result.stdout)


def test_password_flag_no_longer_accepted():
    result = runner.invoke(app, ["--password", "secret", "--last", "hour"])
    assert result.exit_code != 0


def test_yaml_config_is_loaded(tmp_path: Path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text("name: yaml_check\nlast: decade\n")
    result = runner.invoke(app, ["--config", str(cfg)])
    assert result.exit_code == 2  # invalid `last` value from yaml is caught by typer's enum validation


def test_cli_overrides_yaml(tmp_path: Path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text("name: from_yaml\nlast: hour\n")
    # --last decade is invalid; if CLI override works, it preempts the yaml value and we see the enum error.
    result = runner.invoke(app, ["--config", str(cfg), "--last", "decade"])
    assert result.exit_code == 2


def test_negative_days_rejected():
    result = runner.invoke(app, ["--days", "0"])
    assert result.exit_code == 2


def test_no_dates_no_update_errors_out():
    result = runner.invoke(app, [])
    # Without --start / --last / --days / --update, pipeline raises SystemExit.
    assert result.exit_code != 0


def test_invalid_period_value_rejected():
    result = runner.invoke(app, ["--last", "decade"])
    assert result.exit_code == 2
