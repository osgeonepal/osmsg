import csv
import json
import os
import urllib.parse
from typing import Any, Dict, List, Optional
import humanize
import pandas as pd
import duckdb
from shapely.geometry import Point
from shapely.strtree import STRtree
import traceback
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

from .utils import (
    create_profile_link,
    create_charts,
    extract_projects,
    generate_tm_stats,
    sum_tags,
    update_stats,
    update_summary,
)


# Core query
def get_user_stats(
    conn: duckdb.DuckDBPyConnection,
    include_metadata: bool = False,
    additional_tags: Optional[List[str]] = None,
    all_tags: bool = False,
    key_value: bool = False,
    length_tags: Optional[List[str]] = None,
    top_n: Optional[int] = None,
    countries_gdf=None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Query DuckDB and return user-level aggregated stats as list[dict].
    Returns list[dict]
    """
    try:
        # Base query
        base_rows = conn.execute("""
            SELECT
                u.uid,
                u.username                                              AS name,
                COUNT(DISTINCT cs.changeset_id)                        AS changesets,
                SUM(cs.nodes_created)                                  AS nodes_create,
                SUM(cs.nodes_modified)                                 AS nodes_modify,
                SUM(cs.nodes_deleted)                                  AS nodes_delete,
                SUM(cs.ways_created)                                   AS ways_create,
                SUM(cs.ways_modified)                                  AS ways_modify,
                SUM(cs.ways_deleted)                                   AS ways_delete,
                SUM(cs.rels_created)                                   AS rels_create,
                SUM(cs.rels_modified)                                  AS rels_modify,
                SUM(cs.rels_deleted)                                   AS rels_delete,
                SUM(cs.poi_created)                                    AS poi_create,
                SUM(cs.poi_modified)                                   AS poi_modify,
                SUM(
                    cs.nodes_created + cs.nodes_modified + cs.nodes_deleted +
                    cs.ways_created  + cs.ways_modified  + cs.ways_deleted  +
                    cs.rels_created  + cs.rels_modified  + cs.rels_deleted
                )                                                      AS map_changes
            FROM users u
            JOIN changeset_stats cs ON u.uid = cs.uid
            GROUP BY u.uid, u.username
            ORDER BY map_changes DESC
        """).fetchall()

        if not base_rows:
            print("No data found in DuckDB")
            return None

        base_cols = [
            "uid",
            "name",
            "changesets",
            "nodes_create",
            "nodes_modify",
            "nodes_delete",
            "ways_create",
            "ways_modify",
            "ways_delete",
            "rels_create",
            "rels_modify",
            "rels_delete",
            "poi_create",
            "poi_modify",
            "map_changes",
        ]
        rows: List[Dict[str, Any]] = [dict(zip(base_cols, r)) for r in base_rows]

        # Apply top_n before heavier post-processing
        if top_n:
            rows = rows[:top_n]

        uid_index: Dict[int, Dict[str, Any]] = {r["uid"]: r for r in rows}

        # Metadata: hashtags, editors, countries
        if include_metadata:
            attach_metadata(conn, rows, uid_index, countries_gdf)

        # Tag stats: --tags / --all_tags / --length
        if additional_tags or all_tags or length_tags:
            attach_tag_stats(
                conn,
                rows,
                uid_index,
                additional_tags=additional_tags,
                all_tags=all_tags,
                key_value=key_value,
                length_tags=length_tags,
            )

        # Add rank
        for i, r in enumerate(rows, 1):
            r["rank"] = i

        return rows

    except Exception as e:
        print(f"Error querying user stats: {e}")

        traceback.print_exc()
        return None


# Metadata attachment
def attach_metadata(conn, rows, uid_index, countries_gdf):
    """Attach hashtags, editors, countries to each user row."""
    # hashtags : distinct per uid
    try:
        ht_rows = conn.execute("""
            SELECT uid, LIST(DISTINCT hashtag) AS hashtags
            FROM (
                SELECT cset.uid, UNNEST(cset.hashtags) AS hashtag
                FROM changesets cset
                WHERE cset.hashtags IS NOT NULL
                  AND len(cset.hashtags) > 0
            )
            GROUP BY uid
        """).fetchall()
        for uid, hashtags in ht_rows:
            if uid in uid_index:
                uid_index[uid]["hashtags"] = hashtags or []
    except Exception as e:
        print(f"Warning: hashtag query failed: {e}")

    # editors — distinct per uid
    try:
        ed_rows = conn.execute("""
            SELECT uid, LIST(DISTINCT editor) AS editors
            FROM changesets
            WHERE editor IS NOT NULL
            GROUP BY uid
        """).fetchall()
        for uid, editors in ed_rows:
            if uid in uid_index:
                uid_index[uid]["editors"] = editors or []
    except Exception as e:
        print(f"Warning: editor query failed: {e}")

    # Defaults for users with no changeset metadata rows
    for r in rows:
        r.setdefault("hashtags", [])
        r.setdefault("editors", [])
        r["countries"] = []

    # Countries — STRtree spatial join using already-loaded countries_gdf
    if countries_gdf is not None:
        attach_countries(conn, uid_index, countries_gdf)


def attach_countries(conn, uid_index, countries_gdf):
    """
    Query bbox centroids from changesets, intersect with countries_gdf
    via a Shapely STRtree.
    """
    try:

        bbox_rows = conn.execute("""
            SELECT uid,
                   ST_X(ST_Centroid(bbox)) AS lon,
                   ST_Y(ST_Centroid(bbox)) AS lat
            FROM changesets
            WHERE bbox IS NOT NULL
        """).fetchall()

        if not bbox_rows:
            return

        country_geoms = list(countries_gdf.geometry)
        country_names = countries_gdf["name"].tolist()
        tree = STRtree(country_geoms)

        # Group centroids by uid
        uid_centroids: Dict[int, List] = {}
        for uid, lon, lat in bbox_rows:
            if uid in uid_index and lon is not None and lat is not None:
                uid_centroids.setdefault(uid, []).append((lon, lat))

        for uid, centroids in uid_centroids.items():
            found: set = set()
            for lon, lat in centroids:
                pt = Point(lon, lat)
                for idx in tree.query(pt, predicate="intersects"):
                    found.add(country_names[idx])
            uid_index[uid]["countries"] = sorted(found)

    except Exception as e:
        print(f"Warning: country detection failed: {e}")


# Tag stats attachment


def attach_tag_stats(
    conn,
    rows,
    uid_index,
    additional_tags: Optional[List[str]],
    all_tags: bool,
    key_value: bool,
    length_tags: Optional[List[str]],
):
    tag_rows = conn.execute("""
        SELECT uid, tag_stats
        FROM changeset_stats
        WHERE tag_stats IS NOT NULL AND tag_stats != '{}'
    """).fetchall()

    if not tag_rows:
        set_tag_defaults(rows, additional_tags, all_tags, length_tags)
        return

    # uid_agg[uid][key][value] = {"c": int, "m": int, "len": float|None}
    uid_agg: Dict[int, Dict[str, Dict[str, Dict]]] = {}

    for uid, tag_stats_raw in tag_rows:
        if uid not in uid_index:
            continue
        if not tag_stats_raw:
            continue

        try:
            tag_stats = json.loads(tag_stats_raw) if isinstance(tag_stats_raw, str) else tag_stats_raw
        except (json.JSONDecodeError, TypeError):
            continue

        agg = uid_agg.setdefault(uid, {})
        for key, val_dict in tag_stats.items():
            key_agg = agg.setdefault(key, {})
            for val, stat in val_dict.items():
                entry = key_agg.setdefault(val, {"c": 0, "m": 0, "len": None})
                entry["c"] += stat.get("c", 0)
                entry["m"] += stat.get("m", 0)
                raw_len = stat.get("len")
                if raw_len is not None:
                    entry["len"] = (entry["len"] or 0.0) + raw_len

    # Attach to rows
    for uid, agg in uid_agg.items():
        row = uid_index[uid]

        if additional_tags:
            for tag_key in additional_tags:
                if tag_key in agg:
                    row[f"{tag_key}_create"] = sum(v["c"] for v in agg[tag_key].values())
                    row[f"{tag_key}_modify"] = sum(v["m"] for v in agg[tag_key].values())

        if all_tags:
            tags_create: Dict[str, int] = {}
            tags_modify: Dict[str, int] = {}
            for key, val_dict in agg.items():
                tags_create[key] = sum(v["c"] for v in val_dict.values())
                tags_modify[key] = sum(v["m"] for v in val_dict.values())
                if key_value:
                    for val, stat in val_dict.items():
                        kv = f"{key}={val}"
                        tags_create[kv] = tags_create.get(kv, 0) + stat["c"]
                        tags_modify[kv] = tags_modify.get(kv, 0) + stat["m"]
            row["tags_create"] = dict(sorted(tags_create.items(), key=lambda x: x[1], reverse=True))
            row["tags_modify"] = dict(sorted(tags_modify.items(), key=lambda x: x[1], reverse=True))

        if length_tags:
            for tag_key in length_tags:
                if tag_key in agg:
                    total_len = sum((v["len"] or 0.0) for v in agg[tag_key].values())
                    row[f"{tag_key}_len_m"] = round(total_len)

    set_tag_defaults(rows, additional_tags, all_tags, length_tags)


def set_tag_defaults(rows, additional_tags, all_tags, length_tags):
    """Fill zeros/empty dicts for users that had no matching tag data."""
    for r in rows:
        if additional_tags:
            for tag_key in additional_tags:
                r.setdefault(f"{tag_key}_create", 0)
                r.setdefault(f"{tag_key}_modify", 0)
        if all_tags:
            r.setdefault("tags_create", {})
            r.setdefault("tags_modify", {})
        if length_tags:
            for tag_key in length_tags:
                r.setdefault(f"{tag_key}_len_m", 0)


# Summary (--summary flag)


def get_summary_by_day(
    conn: duckdb.DuckDBPyConnection,
    additional_tags: Optional[List[str]] = None,
    all_tags: bool = False,
    key_value: bool = False,
    length_tags: Optional[List[str]] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Daily aggregation grouped by UTC date (format: 2026-04-05).
    Requires changesets table to be populated (--changeset or --hashtags flag).
    Returns list[dict], one entry per day, sorted ascending.

    Optional columns (mirrors get_user_stats):
    - additional_tags  -> tag_{key}_create, tag_{key}_modify per day
    - all_tags         -> tags_create (dict), tags_modify (dict) per day
    - key_value        -> tags_create also includes "key=value" entries
    - length_tags      -> tag_{key}_len_m per day
    - editors          -> always included when changesets table is populated
    """
    try:
        # Base daily aggregation
        rows = conn.execute("""
            SELECT
                CAST(DATE_TRUNC('day', cset.created_at) AS DATE)::VARCHAR  AS date,
                COUNT(DISTINCT cset.changeset_id)                           AS changesets,
                COUNT(DISTINCT cset.uid)                                    AS users,
                SUM(st.nodes_created)                                       AS nodes_create,
                SUM(st.nodes_modified)                                      AS nodes_modify,
                SUM(st.nodes_deleted)                                       AS nodes_delete,
                SUM(st.ways_created)                                        AS ways_create,
                SUM(st.ways_modified)                                       AS ways_modify,
                SUM(st.ways_deleted)                                        AS ways_delete,
                SUM(st.rels_created)                                        AS rels_create,
                SUM(st.rels_modified)                                       AS rels_modify,
                SUM(st.rels_deleted)                                        AS rels_delete,
                SUM(st.poi_created)                                         AS poi_create,
                SUM(st.poi_modified)                                        AS poi_modify,
                SUM(
                    st.nodes_created + st.nodes_modified + st.nodes_deleted +
                    st.ways_created  + st.ways_modified  + st.ways_deleted  +
                    st.rels_created  + st.rels_modified  + st.rels_deleted
                )                                                           AS map_changes
            FROM changesets cset
            JOIN changeset_stats st ON cset.changeset_id = st.changeset_id
            GROUP BY DATE_TRUNC('day', cset.created_at)
            ORDER BY 1
        """).fetchall()

        cols = [
            "date",
            "changesets",
            "users",
            "nodes_create",
            "nodes_modify",
            "nodes_delete",
            "ways_create",
            "ways_modify",
            "ways_delete",
            "rels_create",
            "rels_modify",
            "rels_delete",
            "poi_create",
            "poi_modify",
            "map_changes",
        ]
        result: List[Dict[str, Any]] = [dict(zip(cols, r)) for r in rows]
        if not result:
            return result

        # Index by date string for fast lookup during post-processing
        date_index: Dict[str, Dict] = {r["date"]: r for r in result}

        # Editors per day
        try:
            ed_rows = conn.execute("""
                SELECT
                    CAST(DATE_TRUNC('day', created_at) AS DATE)::VARCHAR AS date,
                    LIST(DISTINCT editor) AS editors
                FROM changesets
                WHERE editor IS NOT NULL
                GROUP BY DATE_TRUNC('day', created_at)
            """).fetchall()
            for date, editors in ed_rows:
                if date in date_index:
                    date_index[date]["editors"] = ",".join(e for e in (editors or []) if e) or None
        except Exception as e:
            print(f"Warning: editors-per-day query failed: {e}")

        # Tag stats per day
        if additional_tags or all_tags or length_tags:
            try:
                tag_rows = conn.execute("""
                    SELECT
                        CAST(DATE_TRUNC('day', cset.created_at) AS DATE)::VARCHAR AS date,
                        st.tag_stats
                    FROM changesets cset
                    JOIN changeset_stats st ON cset.changeset_id = st.changeset_id
                    WHERE st.tag_stats IS NOT NULL AND st.tag_stats != '{}'
                """).fetchall()

                # Accumulate per day: date -> key -> value -> {c, m, len}
                day_agg: Dict[str, Dict[str, Dict[str, Dict]]] = {}
                for date, tag_stats_raw in tag_rows:
                    if date not in date_index:
                        continue
                    try:
                        tag_stats = json.loads(tag_stats_raw) if isinstance(tag_stats_raw, str) else tag_stats_raw
                    except (json.JSONDecodeError, TypeError):
                        continue
                    agg = day_agg.setdefault(date, {})
                    for key, val_dict in tag_stats.items():
                        key_agg = agg.setdefault(key, {})
                        for val, stat in val_dict.items():
                            entry = key_agg.setdefault(val, {"c": 0, "m": 0, "len": None})
                            entry["c"] += stat.get("c", 0)
                            entry["m"] += stat.get("m", 0)
                            raw_len = stat.get("len")
                            if raw_len is not None:
                                entry["len"] = (entry["len"] or 0.0) + raw_len

                # Attach to result rows
                for date, agg in day_agg.items():
                    row = date_index[date]

                    if additional_tags:
                        for tag_key in additional_tags:
                            if tag_key in agg:
                                row[f"{tag_key}_create"] = sum(v["c"] for v in agg[tag_key].values())
                                row[f"{tag_key}_modify"] = sum(v["m"] for v in agg[tag_key].values())

                    if all_tags:
                        tags_create: Dict[str, int] = {}
                        tags_modify: Dict[str, int] = {}
                        for key, val_dict in agg.items():
                            tags_create[key] = sum(v["c"] for v in val_dict.values())
                            tags_modify[key] = sum(v["m"] for v in val_dict.values())
                            if key_value:
                                for val, stat in val_dict.items():
                                    kv = f"{key}={val}"
                                    tags_create[kv] = tags_create.get(kv, 0) + stat["c"]
                                    tags_modify[kv] = tags_modify.get(kv, 0) + stat["m"]
                        row["tags_create"] = dict(sorted(tags_create.items(), key=lambda x: x[1], reverse=True))
                        row["tags_modify"] = dict(sorted(tags_modify.items(), key=lambda x: x[1], reverse=True))

                    if length_tags:
                        for tag_key in length_tags:
                            if tag_key in agg:
                                total_len = sum((v["len"] or 0.0) for v in agg[tag_key].values())
                                row[f"{tag_key}_len_m"] = round(total_len)

            except Exception as e:
                print(f"Warning: tag stats per day query failed: {e}")

            # Defaults for days with no matching tag data
            for r in result:
                if additional_tags:
                    for tag_key in additional_tags:
                        r.setdefault(f"{tag_key}_create", 0)
                        r.setdefault(f"{tag_key}_modify", 0)
                if all_tags:
                    r.setdefault("tags_create", {})
                    r.setdefault("tags_modify", {})
                if length_tags:
                    for tag_key in length_tags:
                        r.setdefault(f"{tag_key}_len_m", 0)

        return result

    except Exception as e:
        print(f"Error getting daily summary: {e}")
        return None


# Export functions
def _ordered_keys(row: Dict) -> List[str]:
    """Consistent column order for CSV / display."""
    priority = [
        "rank",
        "uid",
        "name",
        "profile",
        "changesets",
        "map_changes",
        "nodes_create",
        "nodes_modify",
        "nodes_delete",
        "ways_create",
        "ways_modify",
        "ways_delete",
        "rels_create",
        "rels_modify",
        "rels_delete",
        "poi_create",
        "poi_modify",
        "hashtags",
        "editors",
        "countries",
        "tags_create",
        "tags_modify",
        "start_date",
        "end_date",
    ]
    seen = set(priority)
    # tag_* columns (--tags, --length) come after the fixed block
    extra = [k for k in row if k not in seen]
    return [k for k in priority if k in row] + extra


def _flatten_row_for_export(row: Dict) -> Dict:
    """Convert lists and dicts to strings suitable for flat formats (CSV, Excel)."""
    out = row.copy()
    for col in ("hashtags", "editors", "countries"):
        if col in out and isinstance(out[col], list):
            out[col] = ",".join(out[col])
    for col in ("tags_create", "tags_modify"):
        if col in out and isinstance(out[col], dict):
            out[col] = json.dumps(out[col])
    return out


def export_csv(
    rows: List[Dict[str, Any]],
    output_path: str,
    start_date=None,
    end_date=None,
    include_profile_link: bool = True,
) -> bool:
    """Export to CSV using stdlib csv — no pandas required."""
    try:
        if not rows:
            return False

        out = [_flatten_row_for_export(r) for r in rows]

        for r in out:
            if include_profile_link and "name" in r:
                r["profile"] = create_profile_link(r["name"])
            if start_date is not None:
                r["start_date"] = str(start_date)
            if end_date is not None:
                r["end_date"] = str(end_date)

        fieldnames = _ordered_keys(out[0])

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(out)

        print(f"Exported stats to {output_path}")
        return True

    except Exception as e:
        print(f"Error exporting to CSV: {e}")
        return False


def export_json(
    rows: List[Dict[str, Any]],
    output_path: str,
) -> bool:
    """Export to JSON — lists and dicts kept as-is (proper JSON arrays/objects)."""
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, default=str, indent=2)
        print(f"Exported stats to {output_path}")
        return True
    except Exception as e:
        print(f"Error exporting to JSON: {e}")
        return False


def export_excel(
    rows: List[Dict[str, Any]],
    output_path: str,
) -> bool:
    """Export to Excel — pandas required here only."""
    try:

        out = [_flatten_row_for_export(r) for r in rows]
        df = pd.DataFrame(out, columns=_ordered_keys(out[0]) if out else None)
        df.to_excel(output_path, index=False)
        print(f"Exported stats to {output_path}")
        return True
    except Exception as e:
        print(f"Error exporting to Excel: {e}")
        return False


def export_text(
    rows: List[Dict[str, Any]],
    output_path: str,
    start_date=None,
    end_date=None,
    source_url=None,
) -> bool:
    """Export as markdown grid table — matches previous --text output."""
    try:
        out = [_flatten_row_for_export(r) for r in rows]
        df = pd.DataFrame(out, columns=_ordered_keys(out[0]) if out else None)
        text_output = df.to_markdown(tablefmt="grid", index=False)

        with open(output_path, "w", encoding="utf-8") as f:
            if start_date and end_date:
                header = f"User Contributions From {start_date} to {end_date}"
                if source_url:
                    header += f" . Planet Source File : {source_url}"
                f.write(header + "\n ")
            f.write(text_output)

        print(f"Exported stats to {output_path}")
        return True
    except Exception as e:
        print(f"Error exporting to text: {e}")
        return False


def export_image(
    rows: List[Dict[str, Any]],
    fname: str,
) -> bool:
    """Top-25 users as PNG table — matches previous --image output."""
    try:
        df = pd.DataFrame(rows).head(25).reset_index(drop=True)
        df["Created"] = df["nodes_create"] + df["ways_create"] + df["rels_create"]
        df["Modified"] = df["nodes_modify"] + df["ways_modify"] + df["rels_modify"]
        df["Deleted"] = df["nodes_delete"] + df["ways_delete"] + df["rels_delete"]

        table_df = df[["rank", "name", "changesets", "map_changes", "Created", "Modified", "Deleted"]]

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.axis("off")
        ax.axis("tight")
        table = ax.table(
            cellText=table_df.values,
            colLabels=table_df.columns,
            loc="center",
            fontsize=18,
        )
        fig.text(0.5, 0.95, "Top Users", fontsize=8, ha="center")
        font_props = FontProperties(weight="bold")
        for j in range(table_df.shape[1]):
            table[0, j].set_text_props(fontproperties=font_props)

        out_path = f"{fname}_top_users.png"
        plt.savefig(out_path, bbox_inches="tight", dpi=200)
        plt.close()
        print(f"Exported image to {out_path}")
        return True
    except Exception as e:
        print(f"Error exporting image: {e}")
        return False


# Update helpers


def apply_update_stats(old_csv_path: str, rows: List[Dict]) -> List[Dict]:
    """
    Merge freshly-computed rows with the previous stats CSV by summing numeric
    columns and unioning string columns (countries, hashtags, editors).

    Returns a new list[dict] sorted by map_changes descending with rank reset.
    """
    try:
        old_df = pd.read_csv(old_csv_path, encoding="utf8")
        new_df = pd.DataFrame([_flatten_row_for_export(r) for r in rows])

        merged_df = update_stats(old_df, new_df)

        # update_stats already re-ranks; convert back to list[dict]
        result = merged_df.to_dict(orient="records")
        print(f"Update merged {len(old_df)} old rows + {len(new_df)} new rows → {len(result)} rows")
        return result

    except Exception as e:
        print(f"Warning: apply_update_stats failed, returning new rows only: {e}")
        traceback.print_exc()
        return rows


def apply_update_summary(old_summary_path: str, summary_rows: List[Dict]) -> List[Dict]:
    """
    Merge fresh daily summary rows with the previous summary CSV.

    get_summary_by_day() uses the key 'date'; update_summary() from utils merges
    on 'timestamp'.  We rename before the call and rename back afterwards so both
    sides are compatible.

    Returns a new list[dict] sorted by date ascending.
    """
    try:
        old_df = pd.read_csv(old_summary_path, encoding="utf8")

        # Normalise column name: the old CSV may use 'timestamp' (legacy) or 'date' (new)
        if "timestamp" not in old_df.columns and "date" in old_df.columns:
            old_df = old_df.rename(columns={"date": "timestamp"})

        new_df = pd.DataFrame([_flatten_row_for_export(r) for r in summary_rows])
        if "timestamp" not in new_df.columns and "date" in new_df.columns:
            new_df = new_df.rename(columns={"date": "timestamp"})

        merged_df = update_summary(old_df, new_df)

        # Rename back to 'date' for the rest of the pipeline
        if "timestamp" in merged_df.columns:
            merged_df = merged_df.rename(columns={"timestamp": "date"})

        result = merged_df.to_dict(orient="records")
        print(f"Summary update merged → {len(result)} daily rows")
        return result

    except Exception as e:
        print(f"Warning: apply_update_summary failed, returning new summary only: {e}")
        traceback.print_exc()
        return summary_rows


# Tasking Manager stats


def enrich_with_tm_stats(rows: List[Dict]) -> List[Dict]:
    """
    Extract HOT Tasking Manager project IDs from each user's hashtags, fetch
    per-project contribution stats via the TM API, and attach aggregated totals
    (tm_mapping_level, tasks_mapped, tasks_validated, tasks_total) to each row.

    Rows that have no matching TM data receive zeroed defaults.
    """
    try:
        print("Generating TM Stats ....")

        # Collect unique project IDs across all users
        all_projects: List[str] = []
        for r in rows:
            ht = r.get("hashtags", [])
            if isinstance(ht, list):
                ht_str = ",".join(ht)
            else:
                ht_str = str(ht) if ht else ""
            all_projects.extend(extract_projects(ht_str))

        unique_projects = list(set(all_projects))
        usernames = [r["name"] for r in rows]

        if not unique_projects:
            print("No TM project IDs found in hashtags – skipping TM stats.")
            _set_tm_defaults(rows)
            return rows

        tm_df = generate_tm_stats(unique_projects, usernames)

        if tm_df.empty:
            print("TM API returned no data for the extracted project IDs.")
            _set_tm_defaults(rows)
            return rows

        # Aggregate across projects per user (user may have contributed to many)
        tm_agg = tm_df.groupby("name", as_index=False).agg(
            tm_mapping_level=("tm_mapping_level", "first"),
            tasks_mapped=("tasks_mapped", "sum"),
            tasks_validated=("tasks_validated", "sum"),
            tasks_total=("tasks_total", "sum"),
        )
        tm_lookup: Dict[str, Any] = {row["name"]: row for row in tm_agg.to_dict(orient="records")}

        for r in rows:
            if r["name"] in tm_lookup:
                tm = tm_lookup[r["name"]]
                r["tm_mapping_level"] = tm["tm_mapping_level"]
                r["tasks_mapped"] = int(tm["tasks_mapped"])
                r["tasks_validated"] = int(tm["tasks_validated"])
                r["tasks_total"] = int(tm["tasks_total"])
            else:
                _set_tm_defaults_row(r)

        print(f"TM stats attached for {len(tm_lookup)} users.")
        return rows

    except Exception as e:
        print(f"Warning: enrich_with_tm_stats failed: {e}")
        traceback.print_exc()
        _set_tm_defaults(rows)
        return rows


def _set_tm_defaults(rows: List[Dict]) -> None:
    for r in rows:
        _set_tm_defaults_row(r)


def _set_tm_defaults_row(r: Dict) -> None:
    r.setdefault("tm_mapping_level", None)
    r.setdefault("tasks_mapped", 0)
    r.setdefault("tasks_validated", 0)
    r.setdefault("tasks_total", 0)


# Charts


def export_charts(
    rows: List[Dict],
    fname: str,
    start_date,
    end_date,
) -> List[str]:
    try:
        # Flatten lists/dicts in place before building the DataFrame
        chart_rows = []
        for r in rows:
            flat = r.copy()
            for col in ("hashtags", "editors", "countries"):
                if col in flat:
                    val = flat[col]
                    if isinstance(val, list):
                        joined = ",".join(str(v) for v in val if str(v).strip())
                        flat[col] = joined if joined else None
                    elif not val:
                        flat[col] = None
            for col in ("tags_create", "tags_modify"):
                if col in flat and isinstance(flat[col], dict):
                    flat[col] = json.dumps(flat[col])
            chart_rows.append(flat)

        df = pd.DataFrame(chart_rows).rename(
            columns={
                "nodes_create": "nodes.create",
                "nodes_modify": "nodes.modify",
                "nodes_delete": "nodes.delete",
                "ways_create": "ways.create",
                "ways_modify": "ways.modify",
                "ways_delete": "ways.delete",
                "rels_create": "relations.create",
                "rels_modify": "relations.modify",
                "rels_delete": "relations.delete",
            }
        )
        df["start_date"] = str(start_date)
        df["end_date"] = str(end_date)

        produced = create_charts(df, fname)
        print(f"Charts exported: {produced}")
        return produced or []

    except Exception as e:
        print(f"Error exporting charts: {e}")
        traceback.print_exc()
        return []


# Summary markdown file


def export_summary_md(
    rows: List[Dict],
    summary_rows: Optional[List[Dict]],
    fname: str,
    start_date_display,
    end_date_display,
    additional_tags: Optional[List[str]] = None,
    length_tags: Optional[List[str]] = None,
    all_tags: bool = False,
    tm_stats: bool = False,
    produced_charts: Optional[List[str]] = None,
    base_path: Optional[str] = None,
) -> bool:
    try:
        df = pd.DataFrame(rows)

        # Headline numbers
        n_users = len(df)
        n_changesets = int(df["changesets"].sum()) if "changesets" in df.columns else 0
        n_changes = int(df["map_changes"].sum()) if "map_changes" in df.columns else 0

        created_sum = (
            df.get("nodes_create", 0).fillna(0) + df.get("ways_create", 0).fillna(0) + df.get("rels_create", 0).fillna(0)
        ).sum()
        modified_sum = (
            df.get("nodes_modify", 0).fillna(0) + df.get("ways_modify", 0).fillna(0) + df.get("rels_modify", 0).fillna(0)
        ).sum()
        deleted_sum = (
            df.get("nodes_delete", 0).fillna(0) + df.get("ways_delete", 0).fillna(0) + df.get("rels_delete", 0).fillna(0)
        ).sum()

        summary_text = (
            f"{humanize.intword(n_users)} Users made "
            f"{humanize.intword(n_changesets)} changesets with "
            f"{humanize.intword(n_changes)} map changes."
        )
        thread_summary = (
            f"{humanize.intword(int(created_sum))} OSM Elements were Created, "
            f"{humanize.intword(int(modified_sum))} Modified & "
            f"{humanize.intword(int(deleted_sum))} Deleted."
        )

        with open(f"{fname}_summary.md", "w", encoding="utf-8") as f:

            # Header
            f.write(f"### Last Update : Stats from {start_date_display} to " f"{end_date_display} (UTC Timezone)\n\n")
            f.write(f"#### {summary_text}\n")
            f.write(f"#### {thread_summary}\n")
            f.write(f"Get Full Stats at [stats.csv](/{fname}.csv)\n")
            f.write(f" & Get Summary Stats at [stats_summary.csv](/{fname}_summary.csv)\n")

            # Top 5 users
            df_ranked = df.copy()
            if "rank" not in df_ranked.columns:
                df_ranked.insert(0, "rank", range(1, len(df_ranked) + 1))
            df_ranked = df_ranked.set_index("rank")

            top_n = min(5, len(df_ranked))
            top_users_text = "\nTop 5 Users are : \n"
            for i in range(1, top_n + 1):
                top_users_text += (
                    f"- {df_ranked.loc[i, 'name']} : " f"{humanize.intword(int(df_ranked.loc[i, 'map_changes']))} Map Changes\n"
                )
            f.write(top_users_text)

            # TM top mappers / validators
            if tm_stats and "tasks_mapped" in df.columns:
                top_tm_mappers = "\nTop 5 Tasking Manager Mappers are : \n"
                tm_sort = df.sort_values("tasks_mapped", ascending=False).head(5)
                for _, row in tm_sort.iterrows():
                    top_tm_mappers += f"- {row['name']} : " f"{humanize.intword(int(row.get('tasks_mapped', 0)))} Tasks Mapped\n"
                f.write(top_tm_mappers)

                top_tm_validators = "\nTop 5 Tasking Manager Validators are : \n"
                tm_sort = df.sort_values("tasks_validated", ascending=False).head(5)
                for _, row in tm_sort.iterrows():
                    top_tm_validators += (
                        f"- {row['name']} : " f"{humanize.intword(int(row.get('tasks_validated', 0)))} Tasks Validated\n"
                    )
                f.write(top_tm_validators)

            #  Tag / POI summary
            user_tags_summary = "\nSummary of Supplied Tags\n"

            # POI (always present)
            poi_c = int(df["poi_create"].sum()) if "poi_create" in df.columns else 0
            poi_m = int(df["poi_modify"].sum()) if "poi_modify" in df.columns else 0
            user_tags_summary += f"- poi = Created: {humanize.intword(poi_c)}, " f"Modified : {humanize.intword(poi_m)}\n"

            # Additional tags supplied via --tags
            if additional_tags:
                for tag_key in additional_tags:
                    c_col = f"{tag_key}_create"
                    m_col = f"{tag_key}_modify"
                    t_c = int(df[c_col].sum()) if c_col in df.columns else 0
                    t_m = int(df[m_col].sum()) if m_col in df.columns else 0
                    user_tags_summary += f"- {tag_key} = Created: {humanize.intword(t_c)}, " f"Modified : {humanize.intword(t_m)}\n"

            # Length tags supplied via --length
            if length_tags:
                for len_feat in length_tags:
                    len_col = f"{len_feat}_len_m"
                    total_m = int(df[len_col].sum()) if len_col in df.columns else 0
                    user_tags_summary += f"- {len_feat} length created = " f"{humanize.intword(round(total_m / 1000))} Km\n"

            f.write(f"{user_tags_summary}\n")

            # Top tags from --all_tags
            if all_tags and "tags_create" in df.columns:
                # tags_create may be a dict (not yet flattened) or JSON string
                tags_create_col = df["tags_create"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else (x or "{}")).tolist()
                tag_counts = sum_tags(tags_create_col)
                top_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                created_tags_text = "\nTop 5 Created tags are :\n"
                for tag, count in top_tags:
                    created_tags_text += f"- {tag}: {humanize.intword(count)}\n"
                f.write(f"{created_tags_text}\n")

            if all_tags and "tags_modify" in df.columns:
                tags_modify_col = df["tags_modify"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else (x or "{}")).tolist()
                tag_counts = sum_tags(tags_modify_col)
                top_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                modified_tags_text = "\nTop 5 Modified tags are :\n"
                for tag, count in top_tags:
                    modified_tags_text += f"- {tag}: {humanize.intword(count)}\n"
                f.write(f"{modified_tags_text}\n")

            # Top hashtags
            if "hashtags" in df.columns and df["hashtags"].astype(bool).any():
                top_five = (
                    df["hashtags"]
                    .apply(lambda x: ",".join(x) if isinstance(x, list) else (x or ""))
                    .str.split(",")
                    .explode()
                    .str.strip()
                    .replace("", pd.NA)
                    .dropna()
                    .value_counts()
                    .head(5)
                )
                if not top_five.empty:
                    trending_hashtags = "\nTop 5 trending hashtags are:\n"
                    for ht, cnt in top_five.items():
                        if ht.strip():
                            trending_hashtags += f"- {ht} : {cnt} users\n"
                    f.write(f"{trending_hashtags}\n")

            # Top editors
            if "editors" in df.columns and df["editors"].astype(bool).any():
                top_five = (
                    df["editors"]
                    .apply(lambda x: ",".join(x) if isinstance(x, list) else (x or ""))
                    .str.split(",")
                    .explode()
                    .str.strip()
                    .replace("", pd.NA)
                    .dropna()
                    .value_counts()
                    .head(5)
                )
                if not top_five.empty:
                    trending_editors = "\nTop 5 trending editors are:\n"
                    for ed, cnt in top_five.items():
                        if ed.strip():
                            trending_editors += f"- {ed} : {cnt} users\n"
                    f.write(f"{trending_editors}\n")

            # Top countries
            if "countries" in df.columns and df["countries"].astype(bool).any():
                top_five = (
                    df["countries"]
                    .apply(lambda x: ",".join(x) if isinstance(x, list) else (x or ""))
                    .str.split(",")
                    .explode()
                    .str.strip()
                    .replace("", pd.NA)
                    .dropna()
                    .value_counts()
                    .head(5)
                )
                if not top_five.empty:
                    trending_countries = "\nTop 5 trending Countries where users contributed are:\n"
                    for ctr, cnt in top_five.items():
                        if ctr.strip():
                            trending_countries += f"- {ctr} : {cnt} users\n"
                    f.write(f"{trending_countries}\n")

            # Chart embeds
            if produced_charts:
                _base = base_path or os.getcwd()
                f.write("\n Charts : \n")
                for chart_path in produced_charts:
                    rel = os.path.relpath(os.path.join(os.getcwd(), chart_path), _base)
                    parts = rel.split(os.sep)
                    while parts and parts[0] == "..":
                        parts.pop(0)
                    rel = "./" + os.sep.join(parts)
                    f.write(f"![Alt text]({rel}) \n")

        print(f"Summary markdown exported to {fname}_summary.md")
        return True

    except Exception as e:
        print(f"Error exporting summary markdown: {e}")
        traceback.print_exc()
        return False


# Metadata JSON


def export_metadata(
    fname: str,
    command: str,
    source_url,
    start_date,
    start_seq: int,
    start_seq_url: str,
    end_date,
    end_seq: int,
    end_seq_url: str,
    timezone: str = "UTC",
) -> bool:

    try:
        from .changefiles import seq_to_timestamp

        start_repl_ts = seq_to_timestamp(start_seq_url, timezone)
        end_repl_ts = seq_to_timestamp(end_seq_url, timezone)

        meta = {
            "command": str(command),
            "source": str(source_url),
            "start_date": str(start_date),
            "start_seq": f"{start_seq} = {start_repl_ts}",
            "end_date": str(end_date),
            "end_seq": f"{end_seq} = {end_repl_ts}",
        }

        out_path = f"{fname}_metadata.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, default=str)

        print(f"Metadata exported to {out_path}")
        return True

    except Exception as e:
        print(f"Error exporting metadata: {e}")
        traceback.print_exc()
        return False
