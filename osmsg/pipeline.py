"""End-to-end orchestration: download → process → ingest → query → export."""

from __future__ import annotations

import concurrent.futures
import copy
import datetime as dt
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from platformdirs import user_cache_dir
from shapely.ops import unary_union

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
from .geofabrik import country_geometry, country_update_url
from .replication import SHORTCUTS, ChangesetReplication, changefile_download_urls, resolve_url
from .ui import info, progress_bar, warn

UTC = dt.UTC


def _default_cache_dir() -> Path:
    return Path(user_cache_dir("osmsg"))


def _cpu_count() -> int:
    # sched_getaffinity is cgroup-aware (matters in containers); not present on macOS/Windows.
    sched = getattr(os, "sched_getaffinity", None)
    if sched is not None:
        return len(sched(0))
    return os.cpu_count() or 4


@dataclass
class RunConfig:
    name: str = "stats"
    start_date: dt.datetime | None = None
    end_date: dt.datetime | None = None
    countries: list[str] | None = None
    urls: list[str] = field(default_factory=lambda: ["https://planet.openstreetmap.org/replication/minute"])
    url_explicit: bool = False
    workers: int | None = None
    additional_tags: list[str] | None = None
    hashtags: list[str] | None = None
    length_tags: list[str] | None = None
    users_filter: list[str] | None = None
    tag_mode: str = "none"
    exact_lookup: bool = False
    changeset: bool = False
    summary: bool = False
    boundary: str | None = None
    tm_stats: bool = False
    formats: list[str] = field(default_factory=lambda: ["parquet"])
    update: bool = False
    delete_temp: bool = False
    cache_dir: Path = field(default_factory=_default_cache_dir)
    output_dir: Path = field(default_factory=lambda: Path("."))
    osm_username: str | None = None
    osm_password: str | None = None
    psql_dsn: str | None = None


def _resolve_country_urls(countries: list[str]) -> list[str]:
    return [country_update_url(region) for region in countries]


def _normalize_urls(cfg: RunConfig) -> None:
    # Explicit --url wins over --country's default Geofabrik URL; --country still
    # contributes the boundary geometry filter downstream.
    if cfg.countries and not cfg.url_explicit:
        cfg.urls = _resolve_country_urls(cfg.countries)
        return
    # Order-preserving dedupe: cfg.urls[0] is load-bearing for resume.
    cfg.urls = list(dict.fromkeys(resolve_url(u) for u in cfg.urls))


def _pick_replication_for_span(span: dt.timedelta) -> str:
    span_h = span.total_seconds() / 3600
    if span_h < 6:
        return "minute"
    if span_h < 24 * 7:
        return "hour"
    return "day"


def _auto_switch_replication(cfg: RunConfig, span: dt.timedelta) -> None:
    """Swap a single planet-shortcut --url for the cheapest one that covers `span`."""
    if cfg.url_explicit or cfg.update or cfg.countries or len(cfg.urls) != 1:
        return
    cur = cfg.urls[0]
    if cur not in SHORTCUTS.values():
        return
    target_label = _pick_replication_for_span(span)
    target_url = SHORTCUTS[target_label]
    if target_url == cur:
        return
    cur_label = next(label for label, url in SHORTCUTS.items() if url == cur)
    warn(
        f"Span is {span}; auto-switching --url from '{cur_label}' to '{target_label}' to reduce load. "
        f"Pass --url {cur_label} to keep '{cur_label}'."
    )
    cfg.urls = [target_url]


def _canonical_hashtags(hashtags: list[str]) -> list[str]:
    # Force leading '#' so 'hotosm' and '#hotosm' both match the '#hotosm' tokens in changeset comments.
    return ["#" + h.lstrip("#") for h in hashtags]


def _resolve_url_starts(conn, cfg: RunConfig) -> dict[str, dt.datetime]:
    if cfg.update:
        if not cfg.urls:
            raise OsmsgError("--update requires at least one source URL.")
        starts: dict[str, dt.datetime] = {}
        for url in cfg.urls:
            last = get_state(conn, url)
            if not last:
                known = [r[0] for r in conn.execute("SELECT source_url FROM state").fetchall()]
                hint = (
                    f" Existing state in this DuckDB is for: {', '.join(known)}. "
                    "Re-run --update with one of those URLs, or start fresh under a different --name."
                    if known
                    else " Run osmsg once without --update to seed state."
                )
                raise OsmsgError(
                    f"--update cannot switch replication URL: no prior state for {url}.{hint} "
                    "(Replaying the same window through a different granularity would double-count "
                    "via the changeset_stats (seq_id, changeset_id) key.)"
                )
            starts[url] = last["last_ts"]
        return starts
    if cfg.start_date is None:
        raise OsmsgError("start_date is required. Pass --start, --last, --days, or --update with a prior run.")
    return {url: cfg.start_date for url in cfg.urls}


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
        "tag_mode": cfg.tag_mode,
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
    urls: list[str],
    mode: str,
    max_workers: int,
    cookie: str | None,
    cache_dir: Path,
    label: str,
    description: str = "downloading",
) -> None:
    with (
        progress_bar(len(urls), unit=label, description=description) as advance,
        concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool,
    ):
        for _ in pool.map(lambda u: download_osm_file(u, mode=mode, cookie=cookie, cache_dir=cache_dir), urls):
            advance()


def _process_all(
    items: list,
    *,
    target,
    initializer,
    init_args,
    chunksize: int,
    label: str,
    workers: int,
    extra_iterables: tuple[list, ...] = (),
    description: str = "processing",
) -> None:
    with (
        progress_bar(len(items), unit=label, description=description) as advance,
        concurrent.futures.ProcessPoolExecutor(
            max_workers=workers, initializer=initializer, initargs=init_args
        ) as pool,
    ):
        for _ in pool.map(target, items, *extra_iterables, chunksize=chunksize):
            advance()


def run(cfg: RunConfig) -> dict[str, Any]:
    """Run a full osmsg pipeline. Returns paths + counts."""
    from .workers import (
        init_changefile_worker,
        init_changeset_worker,
        process_changefile,
        process_changeset,
    )

    cfg = copy.deepcopy(cfg)

    info(f"osmsg {__version__}")
    _normalize_urls(cfg)
    if cfg.hashtags:
        cfg.hashtags = _canonical_hashtags(cfg.hashtags)

    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    cs_dir = cfg.cache_dir / "scratch_cs"
    cf_dir = cfg.cache_dir / "scratch_cf"
    # Drop scratch dirs in case a previous run crashed mid-write.
    for scratch in (cs_dir, cf_dir):
        if scratch.exists():
            shutil.rmtree(scratch, ignore_errors=True)

    cookie = _ensure_credentials(cfg)

    db_path = cfg.output_dir / f"{cfg.name}.duckdb"
    if not cfg.update and db_path.exists():
        db_path.unlink()
    conn = dbmod.connect(str(db_path))
    dbmod.create_tables(conn)
    info(f"DuckDB: {db_path}")

    if cfg.end_date is None:
        cfg.end_date = dt.datetime.now(UTC)
    if cfg.start_date is not None:
        _auto_switch_replication(cfg, cfg.end_date - cfg.start_date)

    url_starts = _resolve_url_starts(conn, cfg)
    if cfg.update:
        # Changeset-replication reads one planet-wide source; widest window covers every URL.
        cfg.start_date = min(url_starts.values())
        info(f"--update: resuming each source from its own state row (earliest: {cfg.start_date.isoformat()})")

    # _resolve_url_starts guarantees start_date is set (or raised); narrow for ty.
    assert cfg.start_date is not None
    if cfg.start_date >= cfg.end_date:
        raise OsmsgError("start_date >= end_date — nothing to do.")

    span = cfg.end_date - cfg.start_date
    info(f"Range: {cfg.start_date.isoformat()} → {cfg.end_date.isoformat()} ({span})")
    span_hours = span.total_seconds() / 3600
    # When auto-switch was suppressed (--url explicit, --update, --country, multi-URL), a long
    # span on minute replication still floods the network. Hint the user.
    if span_hours >= 72 and any(u == SHORTCUTS["minute"] for u in cfg.urls):
        warn(
            f"Range spans {span_hours:.0f}h on minute replication "
            f"(~{int(span_hours * 60):,} files). Consider --url hour or --url day."
        )

    geom_wkt = None
    if cfg.boundary:
        cfg.changeset = cfg.changeset or not cfg.hashtags
        geom_wkt = load_boundary(cfg.boundary).wkt
    elif cfg.countries:
        geoms = [country_geometry(r) for r in cfg.countries]
        cfg.changeset = cfg.changeset or not cfg.hashtags
        geom_wkt = (unary_union(geoms) if len(geoms) > 1 else geoms[0]).wkt

    # summary/tm_stats/--all read the changesets table — populate it even if user didn't ask.
    if (cfg.tm_stats or cfg.summary or cfg.tag_mode == "all") and not cfg.changeset and not cfg.hashtags:
        cfg.changeset = True

    max_workers = cfg.workers or _cpu_count()
    info(f"Workers: {max_workers}")

    # None == no filter active; empty set == filter matched nothing (drop everything).
    valid_changesets: set[int] | None = None
    start_seq: int | None = None
    end_seq: int | None = None

    if cfg.hashtags or cfg.changeset:
        cs_repl = ChangesetReplication()
        urls, cs_start, cs_end = cs_repl.download_urls(cfg.start_date, cfg.end_date)
        info(f"Changesets: {len(urls)} files (seq {cs_start}–{cs_end})")

        if urls:
            cs_dir.mkdir(parents=True, exist_ok=True)
            cs_config = _processing_config(cfg, parquet_dir=cs_dir, geom_wkt=geom_wkt)
            cs_config["window_start_utc"] = cfg.start_date.astimezone(UTC)
            cs_config["window_end_utc"] = cfg.end_date.astimezone(UTC)

            _download_all(
                urls, "changeset", max_workers, None, cfg.cache_dir, "changesets", description="Downloading changesets"
            )
            _process_all(
                urls,
                target=process_changeset,
                initializer=init_changeset_worker,
                init_args=(cs_config,),
                chunksize=10,
                label="changesets",
                workers=max_workers,
                description="Processing changesets",
            )
            dbmod.merge_parquet_files(conn, cs_dir, cleanup=True)
            info("Changeset processing complete.")

        if cfg.hashtags or cfg.boundary:
            valid_changesets = set(list_changesets(conn))

    end_date_utc = cfg.end_date.astimezone(UTC)

    for url in cfg.urls:
        info(f"Changefiles ← {url}")
        url_start = url_starts[url]
        urls, server_ts, src_start_seq, src_end_seq, _, _ = changefile_download_urls(url_start, cfg.end_date, url)
        if start_seq is None:
            start_seq = src_start_seq
        end_seq = src_end_seq
        # Cap per URL only — never mutate cfg.end_date or sibling URLs lose their window.
        url_end_date = min(cfg.end_date, server_ts)
        url_start_date_utc = url_start.astimezone(UTC)
        url_end_date_utc = url_end_date.astimezone(UTC)

        if not urls:
            info(f"  {url}: already up-to-date")
            continue

        cf_dir.mkdir(parents=True, exist_ok=True)
        cf_config = _processing_config(cfg, parquet_dir=cf_dir, geom_wkt=None)
        cf_config["start_date_utc"] = url_start_date_utc
        cf_config["end_date_utc"] = url_end_date_utc

        _download_all(
            urls,
            "changefiles",
            max_workers,
            cookie,
            cfg.cache_dir,
            "changefiles",
            description="Downloading changefiles",
        )
        chunksize = 10 if "minute" in url.lower() else 1
        seq_ids = list(range(src_start_seq, src_end_seq + 1))
        _process_all(
            urls,
            target=process_changefile,
            initializer=init_changefile_worker,
            init_args=(valid_changesets, cf_config),
            chunksize=chunksize,
            label="changefiles",
            workers=max_workers,
            extra_iterables=(seq_ids,),
            description="Processing changefiles",
        )
        dbmod.merge_parquet_files(conn, cf_dir, cleanup=True)
        upsert_state(
            conn,
            source_url=url,
            last_seq=src_end_seq,
            last_ts=url_end_date,
            updated_at=dt.datetime.now(UTC),
        )
        info(f"Changefile processing complete: {url}")

    if cfg.delete_temp:
        # Never rmtree cfg.cache_dir itself — it may be the user's platform cache root.
        for sub in (cs_dir, cf_dir, cfg.cache_dir / "changefiles", cfg.cache_dir / "changeset"):
            if sub.exists():
                shutil.rmtree(sub, ignore_errors=True)

    start_date_utc = min(url_starts.values()).astimezone(UTC) if url_starts else cfg.start_date.astimezone(UTC)

    rows = user_stats(conn, top_n=None)
    if not rows:
        dbmod.close(conn)
        # Raised so the CLI can map "no new data" to exit 0.
        raise NoDataFoundError("No stats produced for the requested time range.")

    if cfg.changeset or cfg.hashtags:
        attach_metadata(conn, rows)
    if cfg.additional_tags or cfg.tag_mode != "none" or cfg.length_tags:
        attach_tag_stats(
            conn,
            rows,
            additional_tags=cfg.additional_tags,
            tag_mode=cfg.tag_mode,
            length_tags=cfg.length_tags,
        )

    if cfg.tm_stats:
        rows = tm.enrich(rows)

    out = cfg.output_dir
    written: dict[str, str] = {}
    if "parquet" in cfg.formats:
        written["parquet"] = str(to_parquet(rows, out / f"{cfg.name}.parquet"))
    if "csv" in cfg.formats:
        written["csv"] = str(to_csv(rows, out / f"{cfg.name}.csv"))
    if "json" in cfg.formats:
        written["json"] = str(to_json(rows, out / f"{cfg.name}.json"))

    if "markdown" in cfg.formats:
        from .export.markdown import summary_markdown as render_md

        md_path = out / f"{cfg.name}.md"
        render_md(
            rows,
            output_path=md_path,
            start_date=start_date_utc,
            end_date=end_date_utc,
            additional_tags=cfg.additional_tags,
            length_tags=cfg.length_tags,
            tag_mode=cfg.tag_mode,
            fname=cfg.name,
            tm_stats=cfg.tm_stats,
        )
        written["markdown"] = str(md_path)

    summary_rows: list[dict[str, Any]] | None = None
    if cfg.summary:
        summary_rows = daily_summary(
            conn,
            additional_tags=cfg.additional_tags,
            tag_mode=cfg.tag_mode,
            length_tags=cfg.length_tags,
        )
    if summary_rows:
        if "parquet" in cfg.formats:
            written["summary_parquet"] = str(to_parquet(summary_rows, out / f"{cfg.name}_summary.parquet"))
        if "csv" in cfg.formats:
            written["summary_csv"] = str(to_csv(summary_rows, out / f"{cfg.name}_summary.csv"))
        if "json" in cfg.formats:
            written["summary_json"] = str(to_json(summary_rows, out / f"{cfg.name}_summary.json"))
        if "markdown" in cfg.formats:
            summary_md_path = out / f"{cfg.name}_summary.md"
            summary_markdown(
                rows,
                output_path=summary_md_path,
                start_date=start_date_utc,
                end_date=end_date_utc,
                additional_tags=cfg.additional_tags,
                length_tags=cfg.length_tags,
                tag_mode=cfg.tag_mode,
                fname=cfg.name,
                tm_stats=cfg.tm_stats,
            )
            written["summary_md"] = str(summary_md_path)
        # psql: skipped on purpose — daily_summary is a query over the four base tables.

    if "psql" in cfg.formats:
        if not cfg.psql_dsn:
            raise OsmsgError("'psql' format requires a libpq DSN (--psql-dsn / RunConfig.psql_dsn=...).")
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
