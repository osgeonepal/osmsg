"""Canonical queries against the osmsg DuckDB schema."""

from __future__ import annotations

import json
from typing import Any

import duckdb


def _rows(result) -> list[dict[str, Any]]:
    cols = [d[0] for d in result.description]
    return [dict(zip(cols, r, strict=True)) for r in result.fetchall()]


def user_stats(conn: duckdb.DuckDBPyConnection, top_n: int | None = None) -> list[dict[str, Any]]:
    """One row per user, ranked by total map changes."""
    rows = _rows(
        conn.execute(
            """
            SELECT
                u.uid,
                u.username                                  AS name,
                COUNT(DISTINCT cs.changeset_id)             AS changesets,
                SUM(cs.nodes_created)                       AS nodes_create,
                SUM(cs.nodes_modified)                      AS nodes_modify,
                SUM(cs.nodes_deleted)                       AS nodes_delete,
                SUM(cs.ways_created)                        AS ways_create,
                SUM(cs.ways_modified)                       AS ways_modify,
                SUM(cs.ways_deleted)                        AS ways_delete,
                SUM(cs.rels_created)                        AS rels_create,
                SUM(cs.rels_modified)                       AS rels_modify,
                SUM(cs.rels_deleted)                        AS rels_delete,
                SUM(cs.poi_created)                         AS poi_create,
                SUM(cs.poi_modified)                        AS poi_modify,
                SUM(
                    cs.nodes_created + cs.nodes_modified + cs.nodes_deleted +
                    cs.ways_created  + cs.ways_modified  + cs.ways_deleted  +
                    cs.rels_created  + cs.rels_modified  + cs.rels_deleted
                )                                           AS map_changes
            FROM users u
            JOIN changeset_stats cs ON u.uid = cs.uid
            GROUP BY u.uid, u.username
            ORDER BY map_changes DESC
            """
        )
    )
    if top_n:
        rows = rows[:top_n]
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return rows


def attach_metadata(conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> None:
    """In-place: hashtags + editors per user."""
    if not rows:
        return
    by_uid = {r["uid"]: r for r in rows}
    for r in rows:
        r.setdefault("hashtags", [])
        r.setdefault("editors", [])

    for uid, hashtags in conn.execute(
        """
        SELECT uid, LIST(DISTINCT ht)
        FROM (SELECT uid, UNNEST(hashtags) AS ht FROM changesets WHERE hashtags IS NOT NULL)
        GROUP BY uid
        """
    ).fetchall():
        if uid in by_uid:
            by_uid[uid]["hashtags"] = hashtags or []

    for uid, editors in conn.execute(
        "SELECT uid, LIST(DISTINCT editor) FROM changesets WHERE editor IS NOT NULL GROUP BY uid"
    ).fetchall():
        if uid in by_uid:
            by_uid[uid]["editors"] = editors or []


def _accumulate_tags(
    target: dict[str, Any],
    tag_stats: dict[str, dict[str, dict[str, Any]]],
    *,
    additional_tags: list[str] | None,
    tag_mode: str,
    length_tags: list[str] | None,
) -> None:
    if additional_tags:
        for k in additional_tags:
            vd = tag_stats.get(k)
            if not vd:
                continue
            target[f"{k}_create"] = target.get(f"{k}_create", 0) + sum(int(v.get("c", 0)) for v in vd.values())
            target[f"{k}_modify"] = target.get(f"{k}_modify", 0) + sum(int(v.get("m", 0)) for v in vd.values())
    if length_tags:
        for k in length_tags:
            vd = tag_stats.get(k)
            if not vd:
                continue
            total = sum(float(v.get("len", 0) or 0) for v in vd.values())
            target[f"{k}_len_m"] = round(target.get(f"{k}_len_m", 0) + total)
    if tag_mode != "none":
        tc = target.setdefault("tags_create", {})
        tm = target.setdefault("tags_modify", {})
        for key, vd in tag_stats.items():
            tc[key] = tc.get(key, 0) + sum(int(v.get("c", 0)) for v in vd.values())
            tm[key] = tm.get(key, 0) + sum(int(v.get("m", 0)) for v in vd.values())
            if tag_mode == "all":
                for value, stat in vd.items():
                    kv = f"{key}={value}"
                    tc[kv] = tc.get(kv, 0) + int(stat.get("c", 0))
                    tm[kv] = tm.get(kv, 0) + int(stat.get("m", 0))


def attach_tag_stats(
    conn: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
    *,
    additional_tags: list[str] | None = None,
    tag_mode: str = "none",
    length_tags: list[str] | None = None,
) -> None:
    """In-place: parse the JSON tag_stats column once per row, then aggregate per user."""
    if not rows:
        return
    if not (additional_tags or tag_mode != "none" or length_tags):
        return

    by_uid = {r["uid"]: r for r in rows}
    for r in rows:
        if tag_mode != "none":
            r.setdefault("tags_create", {})
            r.setdefault("tags_modify", {})
        for k in additional_tags or []:
            r.setdefault(f"{k}_create", 0)
            r.setdefault(f"{k}_modify", 0)
        for k in length_tags or []:
            r.setdefault(f"{k}_len_m", 0)

    for uid, tag_stats_json in conn.execute(
        "SELECT uid, tag_stats FROM changeset_stats WHERE tag_stats IS NOT NULL"
    ).fetchall():
        if uid not in by_uid or not tag_stats_json:
            continue
        try:
            payload = json.loads(tag_stats_json) if isinstance(tag_stats_json, str) else tag_stats_json
        except (json.JSONDecodeError, TypeError):
            continue
        _accumulate_tags(
            by_uid[uid],
            payload,
            additional_tags=additional_tags,
            tag_mode=tag_mode,
            length_tags=length_tags,
        )

    if tag_mode != "none":
        for r in rows:
            r["tags_create"] = dict(sorted(r.get("tags_create", {}).items(), key=lambda x: -x[1]))
            r["tags_modify"] = dict(sorted(r.get("tags_modify", {}).items(), key=lambda x: -x[1]))


def daily_summary(
    conn: duckdb.DuckDBPyConnection,
    *,
    additional_tags: list[str] | None = None,
    tag_mode: str = "none",
    length_tags: list[str] | None = None,
) -> list[dict[str, Any]]:
    """One row per UTC day. Requires `changesets` populated (--changeset / --hashtags)."""
    rows = _rows(
        conn.execute(
            """
            SELECT
                CAST(DATE_TRUNC('day', cs.created_at) AS DATE)::VARCHAR AS date,
                COUNT(DISTINCT cs.changeset_id) AS changesets,
                COUNT(DISTINCT cs.uid)          AS users,
                SUM(st.nodes_created)           AS nodes_create,
                SUM(st.nodes_modified)          AS nodes_modify,
                SUM(st.nodes_deleted)           AS nodes_delete,
                SUM(st.ways_created)            AS ways_create,
                SUM(st.ways_modified)           AS ways_modify,
                SUM(st.ways_deleted)            AS ways_delete,
                SUM(st.rels_created)            AS rels_create,
                SUM(st.rels_modified)           AS rels_modify,
                SUM(st.rels_deleted)            AS rels_delete,
                SUM(st.poi_created)             AS poi_create,
                SUM(st.poi_modified)            AS poi_modify,
                SUM(
                    st.nodes_created + st.nodes_modified + st.nodes_deleted +
                    st.ways_created  + st.ways_modified  + st.ways_deleted  +
                    st.rels_created  + st.rels_modified  + st.rels_deleted
                )                               AS map_changes
            FROM changesets cs
            JOIN changeset_stats st ON cs.changeset_id = st.changeset_id
            GROUP BY DATE_TRUNC('day', cs.created_at)
            ORDER BY 1
            """
        )
    )
    if not rows:
        return rows

    by_date = {r["date"]: r for r in rows}

    for date, editors in conn.execute(
        """
        SELECT CAST(DATE_TRUNC('day', created_at) AS DATE)::VARCHAR, LIST(DISTINCT editor)
        FROM changesets WHERE editor IS NOT NULL
        GROUP BY DATE_TRUNC('day', created_at)
        """
    ).fetchall():
        if date in by_date:
            by_date[date]["editors"] = editors or []

    if not (additional_tags or tag_mode != "none" or length_tags):
        return rows

    for r in rows:
        if tag_mode != "none":
            r.setdefault("tags_create", {})
            r.setdefault("tags_modify", {})
        for k in additional_tags or []:
            r.setdefault(f"{k}_create", 0)
            r.setdefault(f"{k}_modify", 0)
        for k in length_tags or []:
            r.setdefault(f"{k}_len_m", 0)

    for date, tag_stats_json in conn.execute(
        """
        SELECT CAST(DATE_TRUNC('day', cs.created_at) AS DATE)::VARCHAR, st.tag_stats
        FROM changesets cs JOIN changeset_stats st ON cs.changeset_id = st.changeset_id
        WHERE st.tag_stats IS NOT NULL
        """
    ).fetchall():
        if date not in by_date or not tag_stats_json:
            continue
        try:
            payload = json.loads(tag_stats_json) if isinstance(tag_stats_json, str) else tag_stats_json
        except (json.JSONDecodeError, TypeError):
            continue
        _accumulate_tags(
            by_date[date],
            payload,
            additional_tags=additional_tags,
            tag_mode=tag_mode,
            length_tags=length_tags,
        )

    if tag_mode != "none":
        for r in rows:
            r["tags_create"] = dict(sorted(r.get("tags_create", {}).items(), key=lambda x: -x[1]))
            r["tags_modify"] = dict(sorted(r.get("tags_modify", {}).items(), key=lambda x: -x[1]))

    return rows


def list_changesets(conn: duckdb.DuckDBPyConnection) -> list[int]:
    return [r[0] for r in conn.execute("SELECT changeset_id FROM changesets").fetchall()]
