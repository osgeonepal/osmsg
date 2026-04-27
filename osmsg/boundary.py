"""Geometry helpers: boundary parsing + bbox centroid."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shapely.geometry import MultiPolygon, Polygon, box, shape
from shapely.geometry.base import BaseGeometry


def load_boundary(input_data: str) -> BaseGeometry:
    """Accept either inline GeoJSON text or a path to a GeoJSON file."""
    try:
        payload: Any = json.loads(input_data)
    except json.JSONDecodeError as exc:
        path = Path(input_data)
        if not path.is_file():
            raise ValueError(f"Not valid JSON or a file path: {input_data!r}") from exc
        payload = json.loads(path.read_text())

    geometry = payload.get("geometry") if "geometry" in payload else payload
    if not geometry or geometry.get("type") not in ("Polygon", "MultiPolygon"):
        raise ValueError("Boundary must be a Polygon or MultiPolygon GeoJSON.")
    geom = shape(geometry)
    if isinstance(geom, (Polygon, MultiPolygon)):
        return geom
    raise ValueError(f"Unexpected geometry type: {type(geom).__name__}")


def bbox_centroid(bounds) -> tuple[float, float] | None:
    """Centroid of an osmium bounding box, or None if invalid."""
    if not bounds.valid():
        return None
    geom = box(bounds.bottom_left.lon, bounds.bottom_left.lat, bounds.top_right.lon, bounds.top_right.lat)
    return geom.centroid.x, geom.centroid.y
