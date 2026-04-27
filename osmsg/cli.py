"""Typer-based CLI for osmsg.

UTC throughout — no display timezone. Outputs default to parquet (queryable from
disk by DuckDB / polars / pandas). Other formats: csv, json, markdown, psql.
"""

from __future__ import annotations

import datetime as dt
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv
from platformdirs import user_cache_dir
from typer_config.decorators import use_yaml_config

from .__version__ import __version__
from .exceptions import (
    CredentialsRequiredError,
    GeofabrikAuthError,
    NoDataFoundError,
    OsmsgError,
    UnknownRegionError,
)
from .pipeline import RunConfig, run
from .ui import console, error, render_table

load_dotenv()
UTC = dt.UTC
DEFAULT_CACHE_DIR = Path(user_cache_dir("osmsg"))

app = typer.Typer(
    add_completion=False,
    no_args_is_help=False,
    help="OpenStreetMap stats generator. Parquet-first, OAuth 2.0, UTC-only.",
)


class Period(StrEnum):
    hour = "hour"
    day = "day"
    week = "week"
    month = "month"
    year = "year"


class Format(StrEnum):
    parquet = "parquet"
    csv = "csv"
    json = "json"
    markdown = "markdown"
    psql = "psql"


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"osmsg {__version__}")
        raise typer.Exit()


def _read_password_stdin() -> str:
    import sys

    pw = sys.stdin.readline().rstrip("\n")
    if not pw:
        error("--password-stdin: no password received on stdin.")
        raise typer.Exit(code=2)
    return pw


def _parse_dt(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = dt.datetime.strptime(value, fmt)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            continue
    raise typer.BadParameter(f"unrecognized datetime: {value!r}")


def _period_range(period: Period) -> tuple[dt.datetime, dt.datetime]:
    now = dt.datetime.now(UTC)
    if period is Period.hour:
        end = now.replace(minute=0, second=0, microsecond=0)
        return end - dt.timedelta(hours=1), end
    if period is Period.day:
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return end - dt.timedelta(days=1), end
    if period is Period.week:
        end = now.replace(hour=0, minute=0, second=0, microsecond=0) - dt.timedelta(days=now.weekday())
        return end - dt.timedelta(days=7), end
    if period is Period.month:
        first_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        prev = first_of_month - dt.timedelta(days=1)
        return prev.replace(day=1), first_of_month
    if period is Period.year:
        first_of_year = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        prev = first_of_year.replace(year=first_of_year.year - 1)
        return prev, first_of_year
    raise ValueError(period)


@app.command()
@use_yaml_config(param_name="config", param_help="YAML config file (CLI flags override its values).")
def main(
    version: Annotated[
        bool | None,
        typer.Option("--version", callback=_version_callback, is_eager=True, help="Print version and exit."),
    ] = None,
    name: Annotated[str, typer.Option(help="Output basename. Writes <name>.duckdb + selected formats.")] = "stats",
    start: Annotated[str | None, typer.Option(help="ISO start (UTC). 'YYYY-MM-DD HH:MM:SS'.")] = None,
    end: Annotated[str | None, typer.Option(help="ISO end (UTC). Defaults to now.")] = None,
    last: Annotated[Period | None, typer.Option(help="Convenience: hour|day|week|month|year.")] = None,
    days: Annotated[int | None, typer.Option(help="Last N days (mutually exclusive with --last).")] = None,
    country: Annotated[
        list[str] | None,
        typer.Option("--country", help="Geofabrik region id(s); resolved live. Requires OSM credentials."),
    ] = None,
    url: Annotated[
        list[str] | None,
        typer.Option("--url", help="Replication URL(s). Shortcuts: minute, hour, day."),
    ] = None,
    hashtags: Annotated[list[str] | None, typer.Option("--hashtags")] = None,
    tags: Annotated[list[str] | None, typer.Option("--tags", help="Per-key counts (e.g. building highway).")] = None,
    length: Annotated[list[str] | None, typer.Option("--length", help="Length-in-m for tag keys.")] = None,
    users: Annotated[
        list[str] | None,
        typer.Option("--users", help="Filter to OSM usernames (case-sensitive, exact match). Repeat for more."),
    ] = None,
    workers: Annotated[int | None, typer.Option(help="Parallel workers (default: cpu count).")] = None,
    rows: Annotated[
        int | None,
        typer.Option(help="Cap rows shown in the console table. Files always carry the full set."),
    ] = None,
    boundary: Annotated[str | None, typer.Option(help="Path to GeoJSON or inline geojson string.")] = None,
    formats: Annotated[list[Format] | None, typer.Option("--format", "-f", help="One or more output formats.")] = None,
    summary: Annotated[bool, typer.Option(help="Also write <name>_summary.parquet + summary.md.")] = False,
    changeset: Annotated[bool, typer.Option(hidden=True)] = False,
    all_tags: Annotated[bool, typer.Option("--all-tags", help="Track every tag key.")] = False,
    key_value: Annotated[bool, typer.Option("--key-value", help="Store key=value combos. Implies --all-tags.")] = False,
    exact_lookup: Annotated[
        bool, typer.Option("--exact-lookup", help="Hashtag whole-word match. Only meaningful with --hashtags.")
    ] = False,
    tm_stats: Annotated[bool, typer.Option("--tm-stats", help="Attach Tasking Manager totals.")] = False,
    update: Annotated[bool, typer.Option(help="Append to existing <name>.duckdb.")] = False,
    cache_dir: Annotated[
        Path, typer.Option("--cache-dir", help="Cache dir for downloaded OSM files.")
    ] = DEFAULT_CACHE_DIR,
    delete_temp: Annotated[bool, typer.Option("--delete-temp", help="Remove cache_dir after processing.")] = False,
    username: Annotated[str | None, typer.Option(help="OSM username. Else $OSM_USERNAME, then prompt.")] = None,
    password_stdin: Annotated[
        bool,
        typer.Option(
            "--password-stdin",
            help="Read OSM password from stdin (one line). Else $OSM_PASSWORD, then prompt.",
        ),
    ] = False,
    psql_dsn: Annotated[str | None, typer.Option("--psql-dsn", help="libpq DSN for --format psql.")] = None,
) -> None:
    """Run osmsg."""
    if formats is None:
        formats = [Format.parquet]
    if sum(1 for x in (start, last, days) if x) > 1:
        error("--start, --last, and --days are mutually exclusive — pick one.")
        raise typer.Exit(code=2)
    if tm_stats and not hashtags:
        from .ui import warn

        warn("--tm-stats has no effect without --hashtags; TM enrichment keys off hashtags.")

    cfg = RunConfig(
        name=name,
        end_date=_parse_dt(end),
        countries=country,
        urls=url or ["minute"],
        workers=workers,
        additional_tags=tags,
        hashtags=hashtags,
        length_tags=length,
        users_filter=users,
        all_tags=all_tags or key_value,
        key_value=key_value,
        exact_lookup=exact_lookup,
        changeset=changeset,
        summary=summary,
        boundary=boundary,
        tm_stats=tm_stats,
        formats=[f.value for f in formats],
        update=update,
        cache_dir=cache_dir,
        delete_temp=delete_temp,
        osm_username=username,
        osm_password=_read_password_stdin() if password_stdin else None,
        psql_dsn=psql_dsn,
    )

    if last is not None:
        cfg.start_date, cfg.end_date = _period_range(last)
    elif days is not None:
        if days <= 0:
            error("--days must be > 0.")
            raise typer.Exit(code=2)
        end_dt = dt.datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        cfg.start_date, cfg.end_date = end_dt - dt.timedelta(days=days), end_dt
    elif start:
        cfg.start_date = _parse_dt(start)

    try:
        result = run(cfg)
    except UnknownRegionError as exc:
        error(f"Unknown region: {exc}")
        raise typer.Exit(code=2) from exc
    except CredentialsRequiredError as exc:
        error(str(exc))
        raise typer.Exit(code=2) from exc
    except GeofabrikAuthError as exc:
        error(f"Geofabrik authentication failed: {exc}")
        raise typer.Exit(code=2) from exc
    except NoDataFoundError as exc:
        # Empty window under --update: exit 0 so cron doesn't flag a no-op as failure.
        from .ui import info as _info

        _info(str(exc))
        raise typer.Exit(code=0) from exc
    except OsmsgError as exc:
        error(str(exc))
        raise typer.Exit(code=2) from exc

    rows_data = result.get("rows_data") or []
    display_n = min(rows or 20, len(rows_data))
    render_table(
        rows_data[:display_n],
        columns=(
            "rank",
            "name",
            "changesets",
            "map_changes",
            "nodes_create",
            "ways_create",
            "rels_create",
            "poi_create",
            "hashtags",
        ),
        title=f"Top users (showing {display_n} of {result['rows']})",
    )
    for label, path in (result.get("files") or {}).items():
        console.print(f"[green]✓[/green] {label}: [bold]{Path(path).name if Path(path).exists() else path}[/bold]")
