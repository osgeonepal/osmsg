from datetime import datetime
from typing import Any

from .db import get_pool

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
        enable_unfiltered_fallback = "FALSE"
    else:
        enable_unfiltered_fallback = "TRUE"

    limit_param = f"${n}"
    n += 1
    offset_param = f"${n}"

    changeset_where = f"WHERE {' AND '.join(changeset_filters)}" if changeset_filters else ""

    tag_ctes = _TAG_CTES if include_tags else ""
    tag_select = "tpu.tag_stats" if include_tags else "NULL::jsonb AS tag_stats"
    tag_join = "LEFT JOIN tag_per_user tpu ON tpu.uid = u.uid" if include_tags else ""
    tag_group = ", tpu.tag_stats" if include_tags else ""

    return f"""
        WITH filtered_changesets AS (
            SELECT changeset_id
            FROM changesets
            {changeset_where}
        ),
        matching_stats AS (
            SELECT st.*
            FROM changeset_stats st
            JOIN filtered_changesets fc ON st.changeset_id = fc.changeset_id
        ),
        stats_scope AS (
            SELECT * FROM matching_stats
            UNION ALL
            SELECT st.*
            FROM changeset_stats st
            WHERE {enable_unfiltered_fallback}
                AND NOT EXISTS (SELECT 1 FROM matching_stats)
        ){tag_ctes}
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
            {tag_select}
        FROM users u
        JOIN stats_scope st ON u.uid = st.uid
        {tag_join}
        GROUP BY u.uid, u.username{tag_group}
        ORDER BY map_changes DESC, u.uid ASC
        LIMIT {limit_param} OFFSET {offset_param}
    """


async def fetch_state() -> dict[str, Any] | None:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow("SELECT last_seq, last_ts, updated_at FROM state ORDER BY updated_at DESC LIMIT 1")
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
