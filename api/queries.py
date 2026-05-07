from datetime import datetime
from typing import Any

from .db import get_pool


def _user_stats_sql(*, filter_dates: bool, filter_hashtags: bool) -> str:
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
        )
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
            ) AS rank
        FROM users u
        JOIN stats_scope st ON u.uid = st.uid
        GROUP BY u.uid, u.username
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
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    filter_dates = start is not None and end is not None
    filter_hashtags = bool(hashtag)
    sql = _user_stats_sql(filter_dates=filter_dates, filter_hashtags=filter_hashtags)
    params: list[Any] = []
    if filter_dates:
        params.extend([start, end])
    if filter_hashtags:
        params.append(hashtag)
    params.extend([limit, offset])

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [dict(row) for row in rows]
