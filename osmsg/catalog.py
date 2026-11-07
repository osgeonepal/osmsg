"""Query sources split at the frontier: history from the `hashtag_changeset` rollup, the recent tail from the
base tables (local DuckDB store or attached Postgres), so `stats.py` runs one query wherever data lives."""

import datetime as dt

from .db.pg import sql_literal
from .stats import COUNT_COLS, MAP_CHANGES_COLS, sum_cols

_PROJECTION = f"changeset_id, uid, editor, created_at, {', '.join(COUNT_COLS)}, tags"


def _pg_ts(value: dt.datetime) -> str:
    return "TIMESTAMPTZ '" + value.astimezone(dt.UTC).strftime("%Y-%m-%d %H:%M:%S%z") + "'"


def _pg_query(attach: str, inner: str) -> str:
    return f"postgres_query('{attach}', '{inner.replace(chr(39), chr(39) * 2)}')"


# Recent side aggregated in Postgres via the changeset_hashtag prefix index; each returns an inlined
# postgres_query relation (no bind params), DISTINCT-deduped so a multi-tag changeset counts once.

_PSUM = sum_cols("s", coalesce=False)
_OSUM = sum_cols(coalesce=False)
_MC = " + ".join(f"s.{c}" for c in MAP_CHANGES_COLS)


def _pg_matched(prefixes, frontier, start, end, hashtag_table="changeset_hashtag", carry_created_at=False) -> str:
    if not prefixes:
        raise ValueError("prefixes must be non-empty")
    ranges = " OR ".join(
        f'(hashtag COLLATE "C" >= {sql_literal(lo)} AND hashtag COLLATE "C" < {sql_literal(hi)})' for lo, hi in prefixes
    )
    bounds = [f"created_at >= {_pg_ts(frontier)}"]
    if start is not None:
        bounds.append(f"created_at >= {_pg_ts(start)}")
    if end is not None:
        bounds.append(f"created_at < {_pg_ts(end)}")
    where = f"({ranges}) AND {' AND '.join(bounds)}"
    if carry_created_at:
        # A changeset's rows all carry its created_at, so min() is that value and trends skip scanning `changesets`.
        return (
            f"SELECT changeset_id, min(created_at) AS created_at "
            f"FROM {hashtag_table} WHERE {where} GROUP BY changeset_id"
        )
    return f"SELECT DISTINCT changeset_id FROM {hashtag_table} WHERE {where}"


def _pg_perchangeset(prefixes, frontier, start, end, extra: str = "") -> str:
    """CTEs `m` (matched changeset ids) and `pc` (one row per changeset: uid, summed counts{extra}),
    the shared basis every recent aggregate groups from."""
    return (
        f"WITH m AS ({_pg_matched(prefixes, frontier, start, end)}), "
        f"pc AS (SELECT s.changeset_id, max(s.uid) AS uid{extra}, {_PSUM} "
        f"FROM m JOIN changeset_stats s USING (changeset_id) GROUP BY s.changeset_id)"
    )


def recent_user_agg(attach, *, prefixes, frontier, start=None, end=None) -> str:
    """Per-uid recent aggregate: changesets + summed counts. Powers summary and leaderboard."""
    inner = (
        f"{_pg_perchangeset(prefixes, frontier, start, end)} "
        f"SELECT uid, count(*) AS changesets, {_OSUM} FROM pc GROUP BY uid"
    )
    return _pg_query(attach, inner)


def recent_tag_agg(attach, *, prefixes, frontier, start=None, end=None) -> str:
    """Per (key, value) recent tag breakdown: creates, modifies, length. Powers the tags endpoint."""
    m = _pg_matched(prefixes, frontier, start, end)
    inner = (
        f"WITH m AS ({m}) "
        f"SELECT (t).k AS k, (t).v AS v, sum((t).c) AS creates, sum((t).m) AS modifies, "
        f"sum((t).l) AS length_m FROM m JOIN changeset_stats s USING (changeset_id), unnest(s.tags) AS t "
        f"GROUP BY (t).k, (t).v"
    )
    return _pg_query(attach, inner)


def recent_editor_agg(attach, *, prefixes, frontier, start=None, end=None) -> str:
    """Per (editor, uid) recent aggregate: changesets (cs), map_changes. Powers the editors endpoint."""
    basis = _pg_perchangeset(prefixes, frontier, start, end, extra=f", max(c.editor) AS editor, sum({_MC}) AS mc")
    basis = basis.replace(
        "FROM m JOIN changeset_stats s USING (changeset_id)",
        "FROM m JOIN changeset_stats s USING (changeset_id) JOIN changesets c USING (changeset_id)",
    )
    inner = (
        f"{basis} SELECT COALESCE(NULLIF(editor, ''), 'unknown') AS editor, uid, count(*) AS cs, "
        f"sum(mc) AS map_changes FROM pc GROUP BY 1, uid"
    )
    return _pg_query(attach, inner)


def recent_bucket_agg(attach, interval: str, *, prefixes, frontier, start=None, end=None) -> str:
    """Per time-bucket recent changesets, distinct users and map_changes; `created_at` comes from the matched
    CTE so only changeset_stats is joined. Buckets are disjoint across the frontier, so they add up."""
    m = _pg_matched(prefixes, frontier, start, end, carry_created_at=True)
    inner = (
        f"WITH m AS ({m}), "
        f"pc AS (SELECT s.changeset_id, max(s.uid) AS uid, m.created_at AS created_at, sum({_MC}) AS mc "
        f"FROM m JOIN changeset_stats s USING (changeset_id) GROUP BY s.changeset_id, m.created_at) "
        f"SELECT to_char(date_trunc({sql_literal(interval)}, created_at), 'YYYY-MM-DD') AS bucket, "
        f"count(*) AS changesets, count(DISTINCT uid) AS users, sum(mc) AS map_changes FROM pc GROUP BY 1"
    )
    return _pg_query(attach, inner)


def recent_cooccur_agg(attach, *, prefixes, frontier, start=None, end=None) -> str:
    """Per (hashtag, uid) recent aggregate over every hashtag the matched changesets carry, for the
    co-occurring hashtags panel."""
    m = _pg_matched(prefixes, frontier, start, end)
    inner = (
        f"WITH m AS ({m}), pc AS (SELECT s.changeset_id, max(s.uid) AS uid, sum({_MC}) AS mc "
        f"FROM m JOIN changeset_stats s USING (changeset_id) GROUP BY s.changeset_id) "
        f"SELECT ch.hashtag AS hashtag, pc.uid AS uid, count(*) AS changesets, sum(pc.mc) AS mc "
        f"FROM pc JOIN changeset_hashtag ch USING (changeset_id) GROUP BY ch.hashtag, pc.uid"
    )
    return _pg_query(attach, inner)


def recent_map_agg(attach, *, prefixes, frontier, start=None, end=None) -> str:
    """Per-changeset recent centroids (changeset_id, uid, lon, lat) from the bbox midpoint. Powers the map."""
    m = _pg_matched(prefixes, frontier, start, end)
    inner = (
        f"WITH m AS ({m}) SELECT c.changeset_id, c.uid, (c.min_lon + c.max_lon) / 2.0 AS lon, "
        f"(c.min_lat + c.max_lat) / 2.0 AS lat FROM m JOIN changesets c USING (changeset_id) "
        f"WHERE c.min_lon IS NOT NULL"
    )
    return _pg_query(attach, inner)


# Whole-OSM (no-hashtag) aggregates over a recent window, from Postgres.


def _pg_global_window(start: dt.datetime, end: dt.datetime) -> str:
    if start is None or end is None:
        raise ValueError("global scope requires a bounded [start, end) window")
    return f"c.created_at >= {_pg_ts(start)} AND c.created_at < {_pg_ts(end)}"


def _pg_uid_list(uids: list[int]) -> str:
    return ", ".join(str(int(u)) for u in uids)


def global_user_agg(attach, *, start, end) -> str:
    inner = (
        f"SELECT s.uid AS uid, count(DISTINCT s.changeset_id) AS changesets, {_PSUM} "
        f"FROM changeset_stats s JOIN changesets c USING (changeset_id) "
        f"WHERE {_pg_global_window(start, end)} GROUP BY s.uid"
    )
    return _pg_query(attach, inner)


def global_editor_agg(attach, *, start, end) -> str:
    inner = (
        f"SELECT COALESCE(NULLIF(c.editor, ''), 'unknown') AS editor, s.uid AS uid, "
        f"count(DISTINCT s.changeset_id) AS cs, sum({_MC}) AS map_changes "
        f"FROM changeset_stats s JOIN changesets c USING (changeset_id) "
        f"WHERE {_pg_global_window(start, end)} GROUP BY 1, s.uid"
    )
    return _pg_query(attach, inner)


def global_tag_agg(attach, *, start, end) -> str:
    inner = (
        "SELECT (t).k AS k, (t).v AS v, sum((t).c) AS creates, sum((t).m) AS modifies, sum((t).l) AS length_m "
        f"FROM changeset_stats s JOIN changesets c USING (changeset_id), unnest(s.tags) AS t "
        f"WHERE {_pg_global_window(start, end)} GROUP BY (t).k, (t).v"
    )
    return _pg_query(attach, inner)


def global_user_tags(attach, uids: list[int], *, start, end) -> str:
    inner = (
        "SELECT s.uid AS uid, (t).k AS k, (t).v AS v, sum((t).c) AS c, sum((t).m) AS m, sum((t).l) AS l "
        f"FROM changeset_stats s JOIN changesets c USING (changeset_id), unnest(s.tags) AS t "
        f"WHERE s.uid IN ({_pg_uid_list(uids)}) AND {_pg_global_window(start, end)} GROUP BY s.uid, (t).k, (t).v"
    )
    return _pg_query(attach, inner)


def global_user_editors(attach, uids: list[int], *, start, end) -> str:
    inner = (
        "SELECT DISTINCT s.uid AS uid, c.editor AS editor "
        "FROM changeset_stats s JOIN changesets c USING (changeset_id) "
        f"WHERE s.uid IN ({_pg_uid_list(uids)}) AND {_pg_global_window(start, end)}"
    )
    return _pg_query(attach, inner)


def global_user_hashtags(attach, uids: list[int], *, start, end) -> str:
    inner = (
        "SELECT DISTINCT s.uid AS uid, lower(h) AS hashtag "
        f"FROM changeset_stats s JOIN changesets c USING (changeset_id), unnest(c.hashtags) AS h "
        f"WHERE s.uid IN ({_pg_uid_list(uids)}) AND {_pg_global_window(start, end)}"
    )
    return _pg_query(attach, inner)


def global_trending(attach, *, start, end) -> str:
    inner = (
        "SELECT lower(h) AS hashtag, count(DISTINCT c.changeset_id) AS changesets, count(DISTINCT c.uid) AS users "
        f"FROM changesets c, unnest(c.hashtags) AS h WHERE {_pg_global_window(start, end)} GROUP BY 1"
    )
    return _pg_query(attach, inner)


def _pg_lower_ranges(col: str, prefixes) -> str:
    return " OR ".join(
        f"(lower({col}) >= {sql_literal(lo)} AND lower({col}) < {sql_literal(hi)})" for lo, hi in prefixes
    )


def _pg_user_window(frontier, start, end) -> str:
    bounds = [f"c.created_at >= {_pg_ts(frontier)}"]
    if start is not None:
        bounds.append(f"c.created_at >= {_pg_ts(start)}")
    if end is not None:
        bounds.append(f"c.created_at < {_pg_ts(end)}")
    return " AND ".join(bounds)


def recent_user_tags(attach, uids: list[int], *, prefixes, frontier, start=None, end=None) -> str:
    """Per (uid, key, value) recent tag breakdown for a fixed set of uids. Driven from the changeset_stats
    uid index (only those users' changesets), so it stays cheap regardless of the hashtag's total size."""
    uid_list = _pg_uid_list(uids)
    ranges = _pg_lower_ranges("h", prefixes)
    inner = (
        f"SELECT s.uid AS uid, (t).k AS k, (t).v AS v, sum((t).c) AS c, sum((t).m) AS m, sum((t).l) AS l "
        f"FROM changeset_stats s JOIN changesets c USING (changeset_id), unnest(s.tags) AS t "
        f"WHERE s.uid IN ({uid_list}) AND {_pg_user_window(frontier, start, end)} "
        f"AND EXISTS (SELECT 1 FROM unnest(c.hashtags) AS h WHERE {ranges}) GROUP BY s.uid, (t).k, (t).v"
    )
    return _pg_query(attach, inner)


def recent_user_cooccur_hashtags(attach, uids: list[int], *, prefixes, frontier, start=None, end=None) -> str:
    """Per (uid, hashtag) recent membership over every hashtag on each user's matched changesets, for the
    profile's co-occurring hashtags."""
    uid_list = _pg_uid_list(uids)
    ranges = _pg_lower_ranges("x", prefixes)
    inner = (
        f"SELECT DISTINCT s.uid AS uid, lower(h) AS hashtag "
        f"FROM changeset_stats s JOIN changesets c USING (changeset_id), unnest(c.hashtags) AS h "
        f"WHERE s.uid IN ({uid_list}) AND {_pg_user_window(frontier, start, end)} "
        f"AND EXISTS (SELECT 1 FROM unnest(c.hashtags) AS x WHERE {ranges})"
    )
    return _pg_query(attach, inner)


def recent_user_editors(attach, uids: list[int], *, prefixes, frontier, start=None, end=None) -> str:
    """Per (uid, editor) recent membership: the distinct editors each user used on their matched changesets."""
    uid_list = _pg_uid_list(uids)
    ranges = _pg_lower_ranges("x", prefixes)
    inner = (
        f"SELECT DISTINCT s.uid AS uid, c.editor AS editor "
        f"FROM changeset_stats s JOIN changesets c USING (changeset_id) "
        f"WHERE s.uid IN ({uid_list}) AND {_pg_user_window(frontier, start, end)} "
        f"AND EXISTS (SELECT 1 FROM unnest(c.hashtags) AS x WHERE {ranges})"
    )
    return _pg_query(attach, inner)


def _window_clause(start: dt.datetime | None, end: dt.datetime | None) -> tuple[str, list[object]]:
    """Optional `created_at` bounds as SQL fragments plus their params. Half-open [start, end), matching the
    frontier split."""
    parts: list[str] = []
    params: list[object] = []
    if start is not None:
        parts.append(" AND created_at >= ?")
        params.append(start)
    if end is not None:
        parts.append(" AND created_at < ?")
        params.append(end)
    return "".join(parts), params


def range_pred(col: str, prefixes: list[tuple[str, str]]) -> str:
    """OR-ed half-open range predicate `(col >= ? AND col < ?)` per prefix. Placeholders bind, in order, to
    the flattened (lo, hi) pairs."""
    return " OR ".join(f"({col} >= ? AND {col} < ?)" for _ in prefixes)


def _prefix_bounds(prefixes: list[tuple[str, str]]) -> list[object]:
    if not prefixes:
        raise ValueError("prefixes must be non-empty")
    return [bound for pair in prefixes for bound in pair]


def _history_filter(prefixes, frontier, start, end) -> tuple[str, list[object]]:
    """WHERE body selecting history rows (< frontier) under the hashtag prefixes and window, with params."""
    window_sql, window_params = _window_clause(start, end)
    where = f"({range_pred('hashtag', prefixes)}) AND created_at < ?{window_sql}"
    return where, [*_prefix_bounds(prefixes), frontier, *window_params]


def _recent_from_base(stats_rel: str, changesets_rel: str, window_sql: str, prefix_pred: str) -> str:
    """Rollup-shaped per-changeset rows from the base tables for matches on or after the frontier.
    Placeholder order: frontier, window bounds, then the prefix bounds `prefix_pred` OR-s together."""
    sums = sum_cols("s", coalesce=False)
    payload = ", ".join(f"p.{c}" for c in COUNT_COLS)
    return f"""
        WITH matched AS (
            SELECT changeset_id, uid, editor, created_at
            FROM {changesets_rel}
            WHERE created_at >= ?{window_sql} AND len(list_filter(hashtags, h -> {prefix_pred})) > 0
        ),
        per_cs AS (
            SELECT s.changeset_id, {sums}
            FROM {stats_rel} s JOIN matched USING (changeset_id) GROUP BY s.changeset_id
        ),
        tag_rows AS (
            SELECT changeset_id, t.k AS k, t.v AS v,
                   SUM(t.c) AS c, SUM(t.m) AS m, SUM(t.l) AS l
            FROM (
                SELECT s.changeset_id, UNNEST(s.tags) AS t
                FROM {stats_rel} s JOIN matched USING (changeset_id)
                WHERE s.tags IS NOT NULL AND len(s.tags) > 0
            )
            GROUP BY changeset_id, t.k, t.v
        ),
        tags AS (
            SELECT changeset_id, list(struct_pack(k := k, v := v, c := c, m := m, l := l)) AS tags
            FROM tag_rows GROUP BY changeset_id
        )
        SELECT m.changeset_id, m.uid, m.editor, m.created_at, {payload}, COALESCE(t.tags, []) AS tags
        FROM matched m JOIN per_cs p USING (changeset_id) LEFT JOIN tags t USING (changeset_id)
    """


def history_dedup_scope(
    history_rel: str,
    *,
    prefixes: list[tuple[str, str]],
    frontier: dt.datetime,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
) -> tuple[str, list[object]]:
    """Deduped per-changeset history rows (< frontier), aggregated separately from the recent side."""
    where, params = _history_filter(prefixes, frontier, start, end)
    return f"SELECT DISTINCT ON (changeset_id) {_PROJECTION} FROM {history_rel} WHERE {where}", params


def history_tag_agg(
    history_rel: str,
    *,
    prefixes: list[tuple[str, str]],
    frontier: dt.datetime,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
) -> tuple[str, list[object]]:
    """Per (key, value) history tag breakdown; unnesting first keeps the per-changeset dedup aggregate narrow
    enough to spill."""
    where, params = _history_filter(prefixes, frontier, start, end)
    sql = (
        "SELECT k, v, SUM(c) AS creates, SUM(m) AS modifies, SUM(l) AS length_m FROM ("
        "SELECT changeset_id, t.k AS k, t.v AS v, ANY_VALUE(t.c) AS c, ANY_VALUE(t.m) AS m, ANY_VALUE(t.l) AS l "
        f"FROM (SELECT changeset_id, UNNEST(tags) AS t FROM {history_rel} WHERE {where}) "
        "GROUP BY changeset_id, k, v) GROUP BY k, v"
    )
    return sql, params


def history_scope_count(
    history_rel: str,
    *,
    prefixes: list[tuple[str, str]],
    frontier: dt.datetime,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
) -> tuple[str, list[object]]:
    """Raw `count(*)` of matching history rows, without dedup so row groups prune; sizes the per-user tag
    breakdown before paying for it."""
    where, params = _history_filter(prefixes, frontier, start, end)
    return f"SELECT count(*) FROM {history_rel} WHERE {where}", params


def recent_scope(
    recent_stats_rel: str,
    recent_changesets_rel: str,
    *,
    prefixes: list[tuple[str, str]],
    frontier: dt.datetime,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
) -> tuple[str, list[object]]:
    """Recent per-changeset rows (created_at >= frontier), one per changeset."""
    window_sql, window_params = _window_clause(start, end)
    recent_pred = range_pred("lower(h)", prefixes)
    sql = "(" + _recent_from_base(recent_stats_rel, recent_changesets_rel, window_sql, recent_pred) + ")"
    return sql, [frontier, *window_params, *_prefix_bounds(prefixes)]


def map_scope(
    history_rollup_rel: str,
    recent_changesets_rel: str,
    *,
    prefixes: list[tuple[str, str]],
    frontier: dt.datetime,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
) -> tuple[str, list[object]]:
    """Deduped changeset centroids `(changeset_id, uid, lon, lat)` for the hashtag union: from the rollup
    `lon`/`lat` (history) and the base changesets bbox midpoint (recent). No-bbox changesets are dropped."""
    hist_where, hist_params = _history_filter(prefixes, frontier, start, end)
    window_sql, window_params = _window_clause(start, end)
    recent_pred = range_pred("lower(h)", prefixes)
    sql = f"""
        SELECT DISTINCT ON (changeset_id) changeset_id, uid, lon, lat FROM (
            SELECT changeset_id, uid, lon, lat FROM {history_rollup_rel}
                WHERE {hist_where} AND lon IS NOT NULL
            UNION ALL
            SELECT changeset_id, uid, (min_lon + max_lon) / 2.0 AS lon, (min_lat + max_lat) / 2.0 AS lat
                FROM {recent_changesets_rel}
                WHERE created_at >= ?{window_sql} AND min_lon IS NOT NULL
                  AND len(list_filter(hashtags, h -> {recent_pred})) > 0
        )
    """
    return sql, [*hist_params, frontier, *window_params, *_prefix_bounds(prefixes)]
