"""Live region lookup via Geofabrik index-v1.json."""

from __future__ import annotations

from functools import lru_cache

from ._http import session
from .exceptions import UnknownRegionError

INDEX_URL = "https://download.geofabrik.de/index-v1.json"


@lru_cache(maxsize=1)
def load_index() -> dict[str, str]:
    """Return `{region_id: updates_url}` parsed from the live Geofabrik index. Cached per process."""
    r = session.get(INDEX_URL, timeout=60)
    r.raise_for_status()
    out: dict[str, str] = {}
    for f in r.json().get("features", []):
        props = f.get("properties") or {}
        rid = props.get("id")
        url = (props.get("urls") or {}).get("updates")
        if rid and url:
            out[rid] = url
    return out


def country_update_url(region_id: str) -> str:
    """Resolve a Geofabrik region id (e.g. ``nepal``) to its `*-updates` base URL.

    Raises:
        UnknownRegion: if the id is not in the live index.
    """
    idx = load_index()
    key = region_id.lower()
    if key not in idx:
        raise UnknownRegionError(f"Geofabrik region '{region_id}' not found")
    return idx[key]


__all__ = ["INDEX_URL", "UnknownRegionError", "country_update_url", "load_index"]
