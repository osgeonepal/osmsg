from datetime import datetime
from typing import Any

from .db import get_pool


def _user_stats_sql(*, filter_hashtags: bool) -> str:
    changeset_filters = ["created_at >= $1", "created_at < $2"]
    if filter_hashtags:
        changeset_filters.append("hashtags && $3::TEXT[]")
        limit_param = "$4"
        offset_param = "$5"
        enable_unfiltered_fallback = "FALSE"
    else:
        limit_param = "$3"
        offset_param = "$4"
        enable_unfiltered_fallback = "TRUE"

    changeset_where = " AND ".join(changeset_filters)

    return f"""
        WITH filtered_changesets AS (
            SELECT changeset_id
            FROM changesets
            WHERE {changeset_where}
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
    start: datetime,
    end: datetime,
    hashtag: list[str] | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    filter_hashtags = bool(hashtag)
    sql = _user_stats_sql(filter_hashtags=filter_hashtags)
    params: list[Any] = [start, end]
    if filter_hashtags:
        params.append(hashtag)
    params.extend([limit, offset])

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [dict(row) for row in rows]
