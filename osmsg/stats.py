"""Single source of truth for how contribution stats are computed: schema and aggregation are fixed, so
the same numbers come out on any engine. Any change to what a stat means happens here alone."""

COUNT_COLS: tuple[str, ...] = (
    "nodes_created",
    "nodes_modified",
    "nodes_deleted",
    "ways_created",
    "ways_modified",
    "ways_deleted",
    "rels_created",
    "rels_modified",
    "rels_deleted",
    "poi_created",
    "poi_modified",
)
# map_changes counts real geometry edits: nodes, ways, relations. poi is a derived tag view, excluded.
MAP_CHANGES_COLS: tuple[str, ...] = tuple(c for c in COUNT_COLS if not c.startswith("poi"))

# A computed way length above this (metres) is a coordinate error, since no OSM way spans 2000 km.
MAX_WAY_LENGTH_M = 2_000_000.0

# Per-changeset tag breakdown in every store: (key, value, creates, modifies, metres `l`); PG_TAG_TYPE in Postgres.
TAG_STRUCT_DDL = "STRUCT(k VARCHAR, v VARCHAR, c BIGINT, m BIGINT, l DOUBLE)"
PG_TAG_TYPE = "osmsg_tag"


def _col(prefix: str, name: str) -> str:
    return f"{prefix}.{name}" if prefix else name


def map_changes_expr(prefix: str = "") -> str:
    """The one definition of map_changes: node + way + relation create/modify/delete."""
    return " + ".join(_col(prefix, c) for c in MAP_CHANGES_COLS)


def sum_cols(prefix: str = "", *, coalesce: bool = True) -> str:
    """`SUM(col) AS col` for every count column, canonical (schema) names."""
    parts = []
    for c in COUNT_COLS:
        expr = f"SUM({_col(prefix, c)})"
        parts.append(f"COALESCE({expr}, 0) AS {c}" if coalesce else f"{expr} AS {c}")
    return ", ".join(parts)


def map_changes_sum(prefix: str = "", *, alias: str = "map_changes") -> str:
    """`SUM(map_changes) AS map_changes`, coalesced."""
    return f"COALESCE(SUM({map_changes_expr(prefix)}), 0) AS {alias}"


def prefix_upper_bound(prefix: str) -> str:
    """Exclusive upper bound for a prefix range scan (last char incremented), so the match stays a range
    predicate the parquet reader can prune on."""
    if not prefix:
        raise ValueError("prefix must be non-empty")
    return prefix[:-1] + chr(ord(prefix[-1]) + 1)
