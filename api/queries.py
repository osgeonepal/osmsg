from datetime import datetime
from typing import Any

from .db import get_pool


def _map_changes_expr(alias: str = "st") -> str:
    return f"""
        {alias}.nodes_created + {alias}.nodes_modified + {alias}.nodes_deleted +
        {alias}.ways_created + {alias}.ways_modified + {alias}.ways_deleted +
        {alias}.rels_created + {alias}.rels_modified + {alias}.rels_deleted
    """


_TAG_CTES = """,
        tag_agg AS (
            SELECT
                st.uid,
                tk.key                                  AS tag_key,
                tv.key                                  AS tag_val,
                SUM(COALESCE((tv.value->>'c')::bigint, 0))  AS total_c,
                SUM(COALESCE((tv.value->>'m')::bigint, 0))  AS total_m,
                SUM((tv.value->>'len')::double precision)   AS total_len
            FROM stats_scope st
            JOIN LATERAL jsonb_each(st.tag_stats) tk ON st.tag_stats IS NOT NULL
            JOIN LATERAL jsonb_each(tk.value)     tv ON true
            GROUP BY st.uid, tk.key, tv.key
        ),
        tag_per_key AS (
            SELECT
                uid,
                tag_key,
                jsonb_object_agg(
                    tag_val,
                    CASE WHEN total_len IS NOT NULL
                        THEN jsonb_build_object('c', total_c, 'm', total_m, 'len', total_len)
                        ELSE jsonb_build_object('c', total_c, 'm', total_m)
                    END
                ) AS tag_vals
            FROM tag_agg
            GROUP BY uid, tag_key
        ),
        tag_per_user AS (
            SELECT uid, jsonb_object_agg(tag_key, tag_vals) AS tag_stats
            FROM tag_per_key
            GROUP BY uid
        )"""

_HASHTAG_CTE = """,
        user_hashtags AS (
            SELECT
                st.uid,
                ARRAY_AGG(DISTINCT ht.hashtag ORDER BY ht.hashtag) AS hashtags
            FROM stats_scope st
            JOIN changesets cs ON cs.changeset_id = st.changeset_id
            CROSS JOIN LATERAL UNNEST(cs.hashtags) AS ht(hashtag)
            WHERE cs.hashtags IS NOT NULL
            GROUP BY st.uid
        )"""


def _user_stats_sql(*, filter_dates: bool, filter_hashtags: bool, include_tags: bool) -> str:
    n = 1
    changeset_filters: list[str] = []

    if filter_dates:
        changeset_filters.append(f"created_at >= ${n}")
        n += 1
        changeset_filters.append(f"created_at < ${n}")
        n += 1

    if filter_hashtags:
        changeset_filters.append(f"hashtags && ${n}::TEXT[]")
        n += 1

    limit_param = f"${n}"
    n += 1
    offset_param = f"${n}"

    # No filter -> all stats (orphans included); any filter -> JOIN through changesets.
    if changeset_filters:
        scope_cte = f"""
        WITH filtered_changesets AS (
            SELECT changeset_id FROM changesets WHERE {" AND ".join(changeset_filters)}
        ),
        stats_scope AS (
            SELECT st.*
            FROM changeset_stats st
            JOIN filtered_changesets fc ON st.changeset_id = fc.changeset_id
        )"""
    else:
        scope_cte = "WITH stats_scope AS (SELECT * FROM changeset_stats)"

    tag_ctes = _TAG_CTES if include_tags else ""
    tag_select = "tpu.tag_stats" if include_tags else "NULL::jsonb AS tag_stats"
    tag_join = "LEFT JOIN tag_per_user tpu ON tpu.uid = u.uid" if include_tags else ""
    tag_group = ", tpu.tag_stats" if include_tags else ""

    return f"""
        {scope_cte}{_HASHTAG_CTE}{tag_ctes}
        SELECT
            u.uid,
            u.username AS name,
            COUNT(DISTINCT st.changeset_id) AS changesets,
            COALESCE(SUM(st.nodes_created), 0) AS nodes_create,
            COALESCE(SUM(st.nodes_modified), 0) AS nodes_modify,
            COALESCE(SUM(st.nodes_deleted), 0) AS nodes_delete,
            COALESCE(SUM(st.ways_created), 0) AS ways_create,
            COALESCE(SUM(st.ways_modified), 0) AS ways_modify,
            COALESCE(SUM(st.ways_deleted), 0) AS ways_delete,
            COALESCE(SUM(st.rels_created), 0) AS rels_create,
            COALESCE(SUM(st.rels_modified), 0) AS rels_modify,
            COALESCE(SUM(st.rels_deleted), 0) AS rels_delete,
            COALESCE(SUM(st.poi_created), 0) AS poi_create,
            COALESCE(SUM(st.poi_modified), 0) AS poi_modify,
            COALESCE(
                SUM(
                    st.nodes_created + st.nodes_modified + st.nodes_deleted +
                    st.ways_created + st.ways_modified + st.ways_deleted +
                    st.rels_created + st.rels_modified + st.rels_deleted
                ),
                0
            ) AS map_changes,
            ROW_NUMBER() OVER (
                ORDER BY
                    COALESCE(
                        SUM(
                            st.nodes_created + st.nodes_modified + st.nodes_deleted +
                            st.ways_created + st.ways_modified + st.ways_deleted +
                            st.rels_created + st.rels_modified + st.rels_deleted
                        ),
                        0
                    ) DESC,
                    u.uid ASC
            ) AS rank,
            COALESCE(uh.hashtags, ARRAY[]::TEXT[]) AS hashtags,
            {tag_select}
        FROM users u
        JOIN stats_scope st ON u.uid = st.uid
        LEFT JOIN user_hashtags uh ON uh.uid = u.uid
        {tag_join}
        GROUP BY u.uid, u.username, uh.hashtags{tag_group}
        ORDER BY map_changes DESC, u.uid ASC
        LIMIT {limit_param} OFFSET {offset_param}
    """


def _changeset_filters_sql(*, filter_dates: bool, filter_hashtags: bool = False) -> tuple[str, int]:
    n = 1
    filters: list[str] = []
    if filter_dates:
        filters.append(f"cs.created_at >= ${n}")
        n += 1
        filters.append(f"cs.created_at < ${n}")
        n += 1
    if filter_hashtags:
        filters.append(f"cs.hashtags && ${n}::TEXT[]")
        n += 1
    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    return where_sql, n


def _hashtag_stats_sql(*, filter_dates: bool, filter_hashtags: bool) -> str:
    where_sql, n = _changeset_filters_sql(filter_dates=filter_dates, filter_hashtags=filter_hashtags)
    limit_param = f"${n}"
    offset_param = f"${n + 1}"
    map_changes = _map_changes_expr()
    return f"""
        WITH hashtag_scope AS (
            SELECT
                ht.hashtag,
                st.uid,
                st.changeset_id,
                ({map_changes}) AS map_changes
            FROM changesets cs
            JOIN changeset_stats st ON st.changeset_id = cs.changeset_id
            CROSS JOIN LATERAL UNNEST(cs.hashtags) AS ht(hashtag)
            {where_sql}
        ),
        hashtag_totals AS (
            SELECT
                hashtag,
                COUNT(DISTINCT changeset_id) AS changesets,
                COUNT(DISTINCT uid) AS users,
                COALESCE(SUM(map_changes), 0) AS map_changes
            FROM hashtag_scope
            GROUP BY hashtag
        )
        SELECT
            hashtag,
            changesets,
            users,
            map_changes,
            ROW_NUMBER() OVER (ORDER BY map_changes DESC, hashtag ASC) AS rank
        FROM hashtag_totals
        ORDER BY map_changes DESC, hashtag ASC
        LIMIT {limit_param} OFFSET {offset_param}
    """


def _hashtag_trends_sql(*, filter_hashtags: bool) -> str:
    where_sql, n = _changeset_filters_sql(filter_dates=True, filter_hashtags=filter_hashtags)
    interval_param = f"${n}"
    limit_param = f"${n + 1}"
    offset_param = f"${n + 2}"
    map_changes = _map_changes_expr()
    return f"""
        SELECT
            DATE_TRUNC({interval_param}, cs.created_at) AS period_start,
            ht.hashtag,
            COUNT(DISTINCT st.changeset_id) AS changesets,
            COUNT(DISTINCT st.uid) AS users,
            COALESCE(SUM({map_changes}), 0) AS map_changes
        FROM changesets cs
        JOIN changeset_stats st ON st.changeset_id = cs.changeset_id
        CROSS JOIN LATERAL UNNEST(cs.hashtags) AS ht(hashtag)
        {where_sql}
        GROUP BY period_start, ht.hashtag
        ORDER BY period_start ASC, map_changes DESC, ht.hashtag ASC
        LIMIT {limit_param} OFFSET {offset_param}
    """


def _editor_stats_sql(*, filter_dates: bool) -> str:
    where_sql, n = _changeset_filters_sql(filter_dates=filter_dates)
    limit_param = f"${n}"
    offset_param = f"${n + 1}"
    map_changes = _map_changes_expr()
    return f"""
        WITH editor_scope AS (
            SELECT
                COALESCE(NULLIF(cs.editor, ''), 'unknown') AS editor,
                st.uid,
                st.changeset_id,
                ({map_changes}) AS map_changes
            FROM changesets cs
            JOIN changeset_stats st ON st.changeset_id = cs.changeset_id
            {where_sql}
        ),
        editor_totals AS (
            SELECT
                editor,
                COUNT(DISTINCT changeset_id) AS changesets,
                COUNT(DISTINCT uid) AS users,
                COALESCE(SUM(map_changes), 0) AS map_changes
            FROM editor_scope
            GROUP BY editor
        )
        SELECT
            editor,
            changesets,
            users,
            map_changes,
            ROW_NUMBER() OVER (ORDER BY map_changes DESC, editor ASC) AS rank
        FROM editor_totals
        ORDER BY map_changes DESC, editor ASC
        LIMIT {limit_param} OFFSET {offset_param}
    """


async def fetch_state() -> dict[str, Any] | None:
    # last_ts/last_seq come from the worst-lagging source (slowest source bounds real freshness);
    # updated_at is the most recent heartbeat across all sources (any tick proves the worker is alive).
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT last_seq, last_ts, (SELECT MAX(updated_at) FROM state) AS updated_at
            FROM state
            ORDER BY last_ts ASC
            LIMIT 1
            """
        )
    if row is None:
        return None
    return dict(row)


async def fetch_user_stats(
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    hashtag: list[str] | None = None,
    tags: bool = True,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    filter_dates = start is not None and end is not None
    filter_hashtags = bool(hashtag)
    sql = _user_stats_sql(filter_dates=filter_dates, filter_hashtags=filter_hashtags, include_tags=tags)
    params: list[Any] = []
    if filter_dates:
        params.extend([start, end])
    if filter_hashtags:
        params.append(hashtag)
    params.extend([limit, offset])

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [dict(row) for row in rows]


async def fetch_hashtag_stats(
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    hashtag: list[str] | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    filter_dates = start is not None and end is not None
    filter_hashtags = bool(hashtag)
    sql = _hashtag_stats_sql(filter_dates=filter_dates, filter_hashtags=filter_hashtags)
    params: list[Any] = []
    if filter_dates:
        params.extend([start, end])
    if filter_hashtags:
        params.append(hashtag)
    params.extend([limit, offset])

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [dict(row) for row in rows]


async def fetch_hashtag_trends(
    *,
    start: datetime,
    end: datetime,
    interval: str,
    hashtag: list[str] | None = None,
    limit: int = 1000,
    offset: int = 0,
) -> list[dict[str, Any]]:
    filter_hashtags = bool(hashtag)
    sql = _hashtag_trends_sql(filter_hashtags=filter_hashtags)
    params: list[Any] = [start, end]
    if filter_hashtags:
        params.append(hashtag)
    params.extend([interval, limit, offset])

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [dict(row) for row in rows]


async def fetch_editor_stats(
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    filter_dates = start is not None and end is not None
    sql = _editor_stats_sql(filter_dates=filter_dates)
    params: list[Any] = []
    if filter_dates:
        params.extend([start, end])
    params.extend([limit, offset])

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [dict(row) for row in rows]
