"""End-to-end orchestration: download → process → ingest → query → export."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from platformdirs import user_cache_dir

from . import db as dbmod
from . import tm
from .__version__ import __version__
from .auth import get_geofabrik_cookie
from .boundary import load_boundary
from .db.queries import attach_metadata, attach_tag_stats, daily_summary, list_changesets, user_stats
from .db.schema import get_state, upsert_state
from .exceptions import CredentialsRequiredError, NoDataFoundError, OsmsgError
from .export import summary_markdown, to_csv, to_json, to_parquet, to_psql
from .fetch import download_osm_file
from .geofabrik import country_update_url
from .replication import ChangesetReplication, changefile_download_urls, resolve_url
from .ui import info, progress_bar

UTC = dt.UTC


def _default_cache_dir() -> Path:
    return Path(user_cache_dir("osmsg"))


@dataclass
class RunConfig:
    name: str = "stats"
    start_date: dt.datetime | None = None
    end_date: dt.datetime | None = None
    countries: list[str] | None = None
    urls: list[str] = field(default_factory=lambda: ["https://planet.openstreetmap.org/replication/minute"])
    workers: int | None = None
    additional_tags: list[str] | None = None
    hashtags: list[str] | None = None
    length_tags: list[str] | None = None
    users_filter: list[str] | None = None
    all_tags: bool = False
    key_value: bool = False
    exact_lookup: bool = False
    changeset: bool = False
    summary: bool = False
    boundary: str | None = None
    tm_stats: bool = False
    formats: list[str] = field(default_factory=lambda: ["parquet"])
    update: bool = False
    delete_temp: bool = False
    cache_dir: Path = field(default_factory=_default_cache_dir)
    osm_username: str | None = None
    osm_password: str | None = None
    psql_dsn: str | None = None


def _resolve_country_urls(countries: list[str]) -> list[str]:
    return [country_update_url(region) for region in countries]


def _normalize_urls(cfg: RunConfig) -> None:
    if cfg.countries:
        cfg.urls = _resolve_country_urls(cfg.countries)
        return
    cfg.urls = list({resolve_url(u) for u in cfg.urls})


def _ensure_credentials(cfg: RunConfig) -> str | None:
    """Resolve OSM credentials and exchange them for a Geofabrik OAuth 2.0 cookie.

    Resolution order: explicit `RunConfig` fields → `OSM_USERNAME` / `OSM_PASSWORD`
    env vars → interactive prompt (only if stdin is a TTY).

    Raises `CredentialsRequiredError` if a geofabrik URL is in use but no credentials
    can be obtained non-interactively (library users running headless).
    """
    if not any("geofabrik" in u.lower() for u in cfg.urls):
        return None

    user = cfg.osm_username or os.environ.get("OSM_USERNAME")
    pw = cfg.osm_password or os.environ.get("OSM_PASSWORD")

    if not user or not pw:
        import sys as _sys

        if not _sys.stdin.isatty():
            raise CredentialsRequiredError(
                "Geofabrik URLs need OSM credentials. Set OSM_USERNAME/OSM_PASSWORD or pass "
                "RunConfig(osm_username=…, osm_password=…)."
            )
        import getpass

        user = user or input("OSM username: ").strip()
        pw = pw or getpass.getpass("OSM password: ")

    info("Authenticating with OSM (OAuth 2.0)…")
    return get_geofabrik_cookie(user, pw)


def _processing_config(cfg: RunConfig, *, parquet_dir: Path, geom_wkt: str | None) -> dict[str, Any]:
    return {
        "hashtags": cfg.hashtags,
        "additional_tags": cfg.additional_tags,
        "all_tags": cfg.all_tags,
        "key_value": cfg.key_value,
        "length": cfg.length_tags,
        "exact_lookup": cfg.exact_lookup,
        "changeset_meta": cfg.changeset,
        "whitelisted_users": cfg.users_filter or [],
        "geom_filter_wkt": geom_wkt,
        "delete_temp": cfg.delete_temp,
        "cache_dir": str(cfg.cache_dir),
        "parquet_dir": str(parquet_dir),
    }


def _download_all(
    urls: list[str], mode: str, max_workers: int, cookie: str | None, cache_dir: Path, label: str
) -> None:
    with (
        progress_bar(len(urls), unit=label) as advance,
        concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool,
    ):
        for _ in pool.map(lambda u: download_osm_file(u, mode=mode, cookie=cookie, cache_dir=cache_dir), urls):
            advance()


def _process_all(urls: list[str], *, target, initializer, init_args, chunksize: int, label: str, workers: int) -> None:
    with (
        progress_bar(len(urls), unit=label) as advance,
        concurrent.futures.ProcessPoolExecutor(
            max_workers=workers, initializer=initializer, initargs=init_args
        ) as pool,
    ):
        for _ in pool.map(target, urls, chunksize=chunksize):
            advance()


def run(cfg: RunConfig) -> dict[str, Any]:
    """Run a full osmsg pipeline. Returns paths + counts."""
    from .workers import (
        init_changefile_worker,
        init_changeset_worker,
        process_changefile,
        process_changeset,
    )

    info(f"osmsg {__version__}")
    _normalize_urls(cfg)

    cs_dir = cfg.cache_dir / "scratch_cs"
    cf_dir = cfg.cache_dir / "scratch_cf"
    # Drop scratch dirs in case a previous run crashed mid-write.
    for scratch in (cs_dir, cf_dir):
        if scratch.exists():
            shutil.rmtree(scratch, ignore_errors=True)

    cookie = _ensure_credentials(cfg)

    db_path = Path(f"{cfg.name}.duckdb")
    if not cfg.update and db_path.exists():
        db_path.unlink()
    conn = dbmod.connect(str(db_path))
    dbmod.create_tables(conn)
    info(f"DuckDB: {db_path}")

    if cfg.update and cfg.start_date is None and cfg.urls:
        last = get_state(conn, cfg.urls[0])
        if last:
            cfg.start_date = last["last_ts"]
            info(f"--update: resuming from {cfg.start_date.isoformat()}")
        else:
            raise OsmsgError(
                f"--update has no prior state for {cfg.urls[0]}. Run osmsg without --update first to seed it."
            )

    if cfg.start_date is None:
        raise OsmsgError("start_date is required. Pass --start, --last, --days, or --update with a prior run.")
    if cfg.end_date is None:
        cfg.end_date = dt.datetime.now(UTC)
    if cfg.start_date == cfg.end_date:
        raise OsmsgError("start_date == end_date — nothing to do.")

    geom_wkt = None
    if cfg.boundary:
        cfg.changeset = cfg.changeset or not cfg.hashtags
        geom_wkt = load_boundary(cfg.boundary).wkt

    # summary/tm_stats read the changesets table — populate it even if user didn't ask.
    if (cfg.tm_stats or cfg.summary) and not cfg.changeset and not cfg.hashtags:
        cfg.changeset = True

    max_workers = cfg.workers or os.cpu_count() or 4
    info(f"Workers: {max_workers}")

    valid_changesets: set[int] = set()
    start_seq: int | None = None
    end_seq: int | None = None

    if cfg.hashtags or cfg.changeset:
        cs_repl = ChangesetReplication()
        urls, cs_start, cs_end = cs_repl.download_urls(cfg.start_date, cfg.end_date)
        info(f"Changesets: {len(urls)} files (seq {cs_start}–{cs_end})")

        if urls:
            cs_dir.mkdir(parents=True, exist_ok=True)
            cs_config = _processing_config(cfg, parquet_dir=cs_dir, geom_wkt=geom_wkt)

            _download_all(urls, "changeset", max_workers, None, cfg.cache_dir, "changesets")
            _process_all(
                urls,
                target=process_changeset,
                initializer=init_changeset_worker,
                init_args=(cs_config,),
                chunksize=10,
                label="changesets",
                workers=max_workers,
            )
            dbmod.merge_parquet_files(conn, cs_dir, cleanup=True)
            info("Changeset processing complete.")

        if cfg.hashtags or cfg.boundary:
            valid_changesets = set(list_changesets(conn))

    start_date_utc = cfg.start_date.astimezone(UTC)
    end_date_utc = cfg.end_date.astimezone(UTC)

    for url in cfg.urls:
        info(f"Changefiles ← {url}")
        urls, server_ts, src_start_seq, src_end_seq, _, _ = changefile_download_urls(cfg.start_date, cfg.end_date, url)
        if start_seq is None:
            start_seq = src_start_seq
        end_seq = src_end_seq
        if server_ts < cfg.end_date:
            cfg.end_date = server_ts
        end_date_utc = cfg.end_date.astimezone(UTC)

        if not urls:
            info(f"  {url}: already up-to-date")
            continue

        cf_dir.mkdir(parents=True, exist_ok=True)
        cf_config = _processing_config(cfg, parquet_dir=cf_dir, geom_wkt=None)
        cf_config["start_date_utc"] = start_date_utc
        cf_config["end_date_utc"] = end_date_utc

        _download_all(urls, "changefiles", max_workers, cookie, cfg.cache_dir, "changefiles")
        chunksize = 10 if "minute" in url.lower() else 1
        _process_all(
            urls,
            target=process_changefile,
            initializer=init_changefile_worker,
            init_args=(valid_changesets, cf_config),
            chunksize=chunksize,
            label="changefiles",
            workers=max_workers,
        )
        dbmod.merge_parquet_files(conn, cf_dir, cleanup=True)
        upsert_state(
            conn,
            source_url=url,
            last_seq=src_end_seq,
            last_ts=end_date_utc,
            updated_at=dt.datetime.now(UTC),
        )
        info(f"Done: {url}")

    if cfg.delete_temp and cfg.cache_dir.exists():
        shutil.rmtree(cfg.cache_dir)

    rows = user_stats(conn, top_n=None)
    if not rows:
        dbmod.close(conn)
        # Raised so the CLI can map "no new data" to exit 0.
        raise NoDataFoundError("No stats produced for the requested time range.")

    if cfg.changeset or cfg.hashtags:
        attach_metadata(conn, rows)
    if cfg.additional_tags or cfg.all_tags or cfg.length_tags:
        attach_tag_stats(
            conn,
            rows,
            additional_tags=cfg.additional_tags,
            all_tags=cfg.all_tags,
            key_value=cfg.key_value,
            length_tags=cfg.length_tags,
        )

    if cfg.tm_stats:
        rows = tm.enrich(rows)

    written: dict[str, str] = {}
    if "parquet" in cfg.formats:
        written["parquet"] = str(to_parquet(rows, Path(f"{cfg.name}.parquet")))
    if "csv" in cfg.formats:
        written["csv"] = str(to_csv(rows, Path(f"{cfg.name}.csv")))
    if "json" in cfg.formats:
        written["json"] = str(to_json(rows, Path(f"{cfg.name}.json")))

    if "markdown" in cfg.formats:
        from .export.markdown import summary_markdown as render_md

        render_md(
            rows,
            output_path=Path(f"{cfg.name}.md"),
            start_date=start_date_utc,
            end_date=end_date_utc,
            additional_tags=cfg.additional_tags,
            length_tags=cfg.length_tags,
            all_tags=cfg.all_tags,
            fname=cfg.name,
            tm_stats=cfg.tm_stats,
        )
        written["markdown"] = f"{cfg.name}.md"

    summary_rows: list[dict[str, Any]] | None = None
    if cfg.summary:
        summary_rows = daily_summary(
            conn,
            additional_tags=cfg.additional_tags,
            all_tags=cfg.all_tags,
            key_value=cfg.key_value,
            length_tags=cfg.length_tags,
        )
    if summary_rows:
        if "parquet" in cfg.formats:
            written["summary_parquet"] = str(to_parquet(summary_rows, Path(f"{cfg.name}_summary.parquet")))
        if "csv" in cfg.formats:
            written["summary_csv"] = str(to_csv(summary_rows, Path(f"{cfg.name}_summary.csv")))
        if "json" in cfg.formats:
            written["summary_json"] = str(to_json(summary_rows, Path(f"{cfg.name}_summary.json")))
        if "markdown" in cfg.formats:
            summary_markdown(
                rows,
                output_path=Path(f"{cfg.name}_summary.md"),
                start_date=start_date_utc,
                end_date=end_date_utc,
                additional_tags=cfg.additional_tags,
                length_tags=cfg.length_tags,
                all_tags=cfg.all_tags,
                fname=cfg.name,
                tm_stats=cfg.tm_stats,
            )
            written["summary_md"] = f"{cfg.name}_summary.md"
        # psql: skipped on purpose — daily_summary is a query over the four base tables.

    if "psql" in cfg.formats:
        if not cfg.psql_dsn:
            raise OsmsgError("'psql' format requires RunConfig.psql_dsn (libpq DSN string).")
        info(f"Pushing to PostgreSQL: {cfg.psql_dsn.split()[0]}…")
        to_psql(conn, cfg.psql_dsn)
        written["psql"] = cfg.psql_dsn

    dbmod.close(conn)
    return {
        "rows": len(rows),
        "files": written,
        "rows_data": rows,
        "summary": summary_rows,
        "start_seq": start_seq,
        "end_seq": end_seq,
    }


__all__ = ["RunConfig", "run"]
