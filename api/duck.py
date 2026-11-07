"""DuckDB query layer: history from the published hashtag_changeset artifact plus the recent tail from the
base Postgres tables; the frontier is re-read on a TTL so a newly published month needs no restart."""

import asyncio
import collections
import contextlib
import datetime as dt
import glob
import logging
import os
import queue
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import duckdb
from litestar.exceptions import HTTPException, TooManyRequestsException

from osmsg import query
from osmsg.db.pg import attach_postgres
from osmsg.db.schema import _apply_runtime_pragmas
from osmsg.history import DEFAULT_HISTORY_URL, fetch_manifest
from osmsg.query import Sources

_log = logging.getLogger("osmsg.api.duck")
_PG_ATTACH = "pg"
_BUSY_DETAIL = "Server is busy, please try again in a moment."

# Warm connections reused so cold-start is paid once; the pool size caps concurrency so heavy queries queue.
_POOL_SIZE = int(os.getenv("OSMSG_DUCKDB_POOL", "3"))
# Max wait for a free slot before shedding with 429 (see _acquire).
_POOL_WAIT = float(os.getenv("OSMSG_POOL_WAIT_SECONDS", "8"))
# Above the slowest legitimate query; the watchdog interrupts anything past it so a runaway cannot pin a core.
_QUERY_TIMEOUT = float(os.getenv("OSMSG_QUERY_TIMEOUT_SECONDS", "150"))
_pool: queue.Queue | None = None
_pool_lock = threading.Lock()

_QUERY_CACHE_DIR = os.getenv("OSMSG_QUERY_CACHE_DIR")  # memoize all-time mega aggregates here; unset disables it
_HISTORY_URL = os.getenv("OSMSG_HISTORY_URL", DEFAULT_HISTORY_URL)
_ROLLUP = os.getenv("OSMSG_ROLLUP_BASE", f"{_HISTORY_URL}/rollup")
_HASHTAG_CHANGESET = os.getenv("OSMSG_HASHTAG_CHANGESET", f"{_ROLLUP}/hashtag_changeset/data.parquet")
_USERS = os.getenv("OSMSG_USERS", f"{_ROLLUP}/users/data.parquet")
_FRONTIER_TTL_SECONDS = int(os.getenv("OSMSG_FRONTIER_TTL_SECONDS", "3600"))
# work_mem for the API's pooled Postgres connections only, sized so the heaviest global aggregate stays in memory.
_PG_WORK_MEM = os.getenv("OSMSG_PG_WORK_MEM", "64MB")

_frontier_cache: tuple[float, dt.datetime] | None = None


def _libpq_dsn() -> str:
    """DuckDB's postgres extension wants a libpq keyword string; the app config is a postgresql:// URL."""
    u = urlparse(os.environ["DATABASE_URL"])
    parts = {
        "host": u.hostname,
        "port": u.port,
        "dbname": u.path.lstrip("/"),
        "user": u.username,
        "password": u.password,
    }
    dsn = " ".join(f"{k}={v}" for k, v in parts.items() if v is not None)
    return f"{dsn} options='-c work_mem={_PG_WORK_MEM}'"


def _frontier() -> dt.datetime:
    """The published frontier, re-read from the manifest at most once per TTL so a newly published
    month advances the split without restarting the API."""
    global _frontier_cache
    now = time.monotonic()
    if _frontier_cache is not None and now - _frontier_cache[0] < _FRONTIER_TTL_SECONDS:
        return _frontier_cache[1]
    manifest = fetch_manifest(_HISTORY_URL)
    if manifest is None:
        raise RuntimeError("history manifest unavailable; set OSMSG_HISTORY_URL")
    _frontier_cache = (now, manifest.frontier)
    return manifest.frontier


_DUCKDB_TEMP_BASE = os.environ.get("OSMSG_DUCKDB_TEMP_DIR")


def _isolate_temp_dir(con: duckdb.DuckDBPyConnection) -> None:
    """Give this pooled connection its own spill directory. Every pooled connection otherwise shares
    OSMSG_DUCKDB_TEMP_DIR, and DuckDB's spill file names are not per-connection, so concurrent spills collide."""
    if not _DUCKDB_TEMP_BASE:
        return
    d = os.path.join(_DUCKDB_TEMP_BASE, f"conn-{uuid.uuid4().hex}")
    os.makedirs(d, exist_ok=True)
    con.execute(f"SET temp_directory='{d.replace(chr(39), chr(39) * 2)}'")


def _sweep_stale_temp_dirs() -> None:
    """Best-effort removal of per-connection spill dirs left by a prior process, run once when the pool builds."""
    if not _DUCKDB_TEMP_BASE:
        return
    for d in glob.glob(os.path.join(_DUCKDB_TEMP_BASE, "conn-*")):
        shutil.rmtree(d, ignore_errors=True)


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL json; LOAD json; INSTALL postgres; LOAD postgres;")
    con.execute("SET http_retries=10;")
    # Memory/temp pragmas so concurrent pooled queries cannot sum past the container memory cap.
    _apply_runtime_pragmas(con)
    _isolate_temp_dir(con)
    attach_postgres(con, _libpq_dsn(), read_only=True)
    return con


def _sources() -> Sources:
    return Sources(
        history_rel=f"read_parquet('{_HASHTAG_CHANGESET}')",
        recent_stats_rel="pg.changeset_stats",
        recent_changesets_rel="pg.changesets",
        frontier=_frontier(),
        users_rel=f"read_parquet('{_USERS}')",
        recent_users_rel="pg.users",
        pg_attach=_PG_ATTACH,
        cache_dir=_QUERY_CACHE_DIR,
    )


def _pool_ready() -> queue.Queue:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _sweep_stale_temp_dirs()
                warm: queue.Queue = queue.Queue()
                for _ in range(_POOL_SIZE):
                    warm.put(_connect())
                _pool = warm
    return _pool


def _acquire(pool: queue.Queue) -> duckdb.DuckDBPyConnection:
    """Borrow a warm connection, waiting at most _POOL_WAIT; past that the box is saturated, so shed with 429
    before requests stack toward the watchdog timeout or an OOM."""
    try:
        return pool.get(timeout=_POOL_WAIT)
    except queue.Empty:
        raise TooManyRequestsException(detail="Server at capacity; retry shortly.") from None


# A timed-out all-time query caches nothing, so it is re-run in the background to fill the cache its retry
# reads; one at a time on its own connection so it cannot starve the pool.
_warm_pool = ThreadPoolExecutor(max_workers=1)
_warm_inflight: set[tuple[str, str, bool]] = set()
_warm_lock = threading.Lock()


def _warm_with(key, label: str, call) -> None:
    try:
        with contextlib.closing(_connect()) as con:
            call(con)
    except (duckdb.Error, RuntimeError) as e:
        _log.warning("cache warm of %s failed: %s", label, e)
    finally:
        with _warm_lock:
            _warm_inflight.discard(key)


def _submit_warm(key, job, *args) -> None:
    """Run job(*args, key) once per key in the background; the job releases the key when it ends."""
    with _warm_lock:
        if key in _warm_inflight:
            return
        _warm_inflight.add(key)
    _warm_pool.submit(job, *args, key)


def _warm(fn, hashtag, kwargs, key) -> None:
    _warm_with(key, f"{fn.__name__} for {hashtag!r}", lambda con: fn(con, hashtag, _sources(), **kwargs))


def _warm_all_time_job(hashtag, exact, key) -> None:
    _warm_with(key, f"all-time {hashtag!r}", lambda con: query.warm_all_time(con, hashtag, _sources(), exact=exact))


def _enqueue_warm(fn, hashtag, kwargs) -> None:
    """Re-run an all-time query off the request path to fill its cache, deduped per key, best-effort.
    No-op when caching is off."""
    if _QUERY_CACHE_DIR is not None:
        _submit_warm((fn.__name__, repr(hashtag), bool(kwargs.get("exact", False))), _warm, fn, hashtag, kwargs)


def _maybe_warm_all_time(hashtag, exact) -> None:
    """Background-warm this hashtag's all-time history caches when any is still cold. No-op when caching
    is off."""
    if _QUERY_CACHE_DIR is None or not query.all_time_warm_pending(_sources(), hashtag, exact=exact):
        return
    key = ("warm_all_time", repr(hashtag), bool(exact))
    _submit_warm(key, _warm_all_time_job, hashtag, exact)


def _run_pooled(call, name: str, on_interrupt=None):
    """Run call(con) on a pooled connection (429 past _POOL_WAIT) under a watchdog that interrupts it past
    _QUERY_TIMEOUT; a dirty or interrupted connection is replaced, never reused."""
    pool = _pool_ready()
    con = _acquire(pool)
    done = threading.Event()
    interrupted = False

    def _watchdog() -> None:
        nonlocal interrupted
        # Re-check done so a query that finished at the boundary never leaves an interrupt on the idle connection.
        if not done.wait(_QUERY_TIMEOUT) and not done.is_set():
            interrupted = True
            with contextlib.suppress(duckdb.Error):
                con.interrupt()

    watcher = threading.Thread(target=_watchdog, daemon=True)
    watcher.start()
    healthy = True
    try:
        return call(con)
    except duckdb.Error as e:
        healthy = False
        if interrupted:
            if on_interrupt is not None:
                on_interrupt()
            raise HTTPException(status_code=503, detail=_BUSY_DETAIL) from None
        if isinstance(e, duckdb.OutOfMemoryException):
            _log.warning("out of memory in %s under load, shedding as 503: %s", name, e)
            raise HTTPException(status_code=503, detail=_BUSY_DETAIL) from None
        _log.exception("duckdb error in %s", name)
        raise
    finally:
        done.set()
        watcher.join()
        # Recycle any connection the watchdog fired on, even if the query returned in the race window.
        if healthy and not interrupted:
            pool.put(con)
        else:
            with contextlib.suppress(duckdb.Error):
                con.close()
            pool.put(_connect())


def _run(fn, hashtag, **kwargs):
    # Only all-time queries hit the shared cache, so only they are worth warming after a timeout.
    all_time = kwargs.get("start") is None and kwargs.get("end") is None
    return _run_pooled(
        lambda con: fn(con, hashtag, _sources(), **kwargs),
        fn.__name__,
        on_interrupt=(lambda: _enqueue_warm(fn, hashtag, kwargs)) if all_time else None,
    )


async def summary(
    hashtag: str | list[str],
    *,
    exact: bool = False,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
):
    return await asyncio.to_thread(_run, query.summary, hashtag, exact=exact, start=start, end=end)


async def leaderboard(
    hashtag: str | list[str],
    *,
    exact: bool = False,
    detail: bool = False,
    page: int = 1,
    page_size: int = query.DEFAULT_PAGE_SIZE,
    sort: str = "map_changes",
    order: str = "desc",
    q: str | None = None,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
):
    res = await asyncio.to_thread(
        _run,
        query.leaderboard,
        hashtag,
        exact=exact,
        detail=detail,
        page=page,
        page_size=page_size,
        sort=sort,
        order=order,
        q=q,
        start=start,
        end=end,
    )
    if start is None and end is None:
        await asyncio.to_thread(_maybe_warm_all_time, hashtag, exact)
    return res


async def user_detail(
    uid: int,
    *,
    hashtag: str | list[str] | None = None,
    exact: bool = False,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
):
    def run(con, s, **kw):
        return query.user_detail(con, uid, s, **kw)

    return await asyncio.to_thread(_run_global, run, hashtag=hashtag, exact=exact, start=start, end=end)


async def tags(
    hashtag: str | list[str],
    *,
    exact: bool = False,
    limit: int = 100,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
):
    return await asyncio.to_thread(_run, query.tags, hashtag, exact=exact, limit=limit, start=start, end=end)


async def editors(
    hashtag: str | list[str],
    *,
    exact: bool = False,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
):
    return await asyncio.to_thread(_run, query.editors, hashtag, exact=exact, start=start, end=end)


async def hashtags(
    hashtag: str | list[str],
    *,
    exact: bool = False,
    limit: int = 15,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
):
    res = await asyncio.to_thread(_run, query.hashtags, hashtag, exact=exact, limit=limit, start=start, end=end)
    if start is None and end is None:
        await asyncio.to_thread(_maybe_warm_all_time, hashtag, exact)
    return res


async def trends(
    hashtag: str | list[str],
    *,
    exact: bool = False,
    interval: str = "day",
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
):
    return await asyncio.to_thread(_run, query.trends, hashtag, exact=exact, interval=interval, start=start, end=end)


async def map_points(
    hashtag: str | list[str],
    *,
    exact: bool = False,
    limit: int = 2000,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
):
    return await asyncio.to_thread(_run, query.map_points, hashtag, exact=exact, limit=limit, start=start, end=end)


def _run_global(fn, **kwargs):
    return _run_pooled(lambda con: fn(con, _sources(), **kwargs), fn.__name__)


# Memoize whole-OSM results by a minute-rounded window so repeat hits are instant.
_GLOBAL_CACHE_TTL = float(os.getenv("OSMSG_GLOBAL_CACHE_TTL", "120"))
_global_cache: collections.OrderedDict[tuple, tuple[float, object]] = collections.OrderedDict()
_global_cache_lock = threading.Lock()


def _round_window(start: dt.datetime, end: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
    return start.replace(second=0, microsecond=0), end.replace(second=0, microsecond=0)


async def _global_cached(fn, key_extra, start, end, **kwargs):
    rs, re = _round_window(start, end)
    key = (fn.__name__, rs, re, key_extra)
    now = time.monotonic()
    with _global_cache_lock:
        hit = _global_cache.get(key)
        if hit and hit[0] > now:
            _global_cache.move_to_end(key)
            return hit[1]
    result = await asyncio.to_thread(_run_global, fn, start=rs, end=re, **kwargs)
    with _global_cache_lock:
        _global_cache[key] = (now + _GLOBAL_CACHE_TTL, result)
        while len(_global_cache) > 512:
            _global_cache.popitem(last=False)
    return result


async def global_summary(*, start: dt.datetime, end: dt.datetime):
    return await _global_cached(query.global_summary, None, start, end)


async def global_leaderboard(
    *,
    start: dt.datetime,
    end: dt.datetime,
    page: int = 1,
    page_size: int = query.DEFAULT_PAGE_SIZE,
    sort: str = "map_changes",
    order: str = "desc",
    q: str | None = None,
    detail: bool = False,
):
    return await _global_cached(
        query.global_leaderboard,
        (page, page_size, sort, order, q, detail),
        start,
        end,
        page=page,
        page_size=page_size,
        sort=sort,
        order=order,
        q=q,
        detail=detail,
    )


async def global_editors(*, start: dt.datetime, end: dt.datetime):
    return await _global_cached(query.global_editors, None, start, end)


async def global_tags(*, start: dt.datetime, end: dt.datetime, limit: int = 100):
    return await _global_cached(query.global_tags, limit, start, end, limit=limit)


async def global_trending(*, start: dt.datetime, end: dt.datetime, limit: int = 15):
    return await _global_cached(query.global_trending, limit, start, end, limit=limit)
