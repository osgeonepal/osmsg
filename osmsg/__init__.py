"""OpenStreetMap stats generator: per-user edit statistics from OSM replication and history."""

from .__version__ import __version__
from .db import (
    attach_metadata,
    attach_tag_stats,
    connect,
    create_tables,
    daily_summary,
    get_state,
    upsert_state,
    user_stats,
)
from .exceptions import (
    CredentialsRequiredError,
    GeofabrikAuthError,
    NoDataFoundError,
    OsmsgError,
    UnknownRegionError,
)
from .export import (
    summary_markdown,
    table_markdown,
    to_csv,
    to_json,
    to_parquet,
    to_psql,
)
from .geofabrik import country_update_url, load_index
from .models import (
    Action,
    Changeset,
    ChangesetStats,
    ElementStat,
    TagValueStat,
    User,
)
from .pipeline import RunConfig, run

__all__ = [
    "Action",
    "Changeset",
    "ChangesetStats",
    "CredentialsRequiredError",
    "ElementStat",
    "GeofabrikAuthError",
    "NoDataFoundError",
    "OsmsgError",
    "RunConfig",
    "TagValueStat",
    "UnknownRegionError",
    "User",
    "__version__",
    "attach_metadata",
    "attach_tag_stats",
    "connect",
    "country_update_url",
    "create_tables",
    "daily_summary",
    "get_state",
    "load_index",
    "run",
    "summary_markdown",
    "table_markdown",
    "to_csv",
    "to_json",
    "to_parquet",
    "to_psql",
    "upsert_state",
    "user_stats",
]
