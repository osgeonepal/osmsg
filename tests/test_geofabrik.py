"""Geofabrik region lookup; `network` tests hit the live index."""

import json

import pytest
from shapely.geometry import MultiPolygon, Polygon

from osmsg import geofabrik
from osmsg.boundary import load_boundary
from osmsg.exceptions import UnknownRegionError
from osmsg.geofabrik import country_geometry, country_update_url, load_index


@pytest.mark.network
def test_country_update_url_resolves_nepal():
    url = country_update_url("nepal")
    assert url.startswith("https://download.geofabrik.de/")
    assert url.endswith("nepal-updates")


@pytest.mark.network
def test_country_update_url_unknown_region():
    with pytest.raises(UnknownRegionError):
        country_update_url("notarealcountry")


def test_load_index_fetches_the_index_once_per_process(monkeypatch):
    calls = []

    class _Index:
        def raise_for_status(self):
            pass

        def json(self):
            return {"features": [{"properties": {"id": "nepal", "urls": {"updates": "https://x/nepal-updates"}}}]}

    def fake_get(url, timeout):
        calls.append(url)
        return _Index()

    monkeypatch.setattr(geofabrik.session, "get", fake_get)
    geofabrik._raw_index.cache_clear()
    try:
        assert load_index() == {"nepal": "https://x/nepal-updates"}
        assert load_index() == {"nepal": "https://x/nepal-updates"}
        assert len(calls) == 1
    finally:
        geofabrik._raw_index.cache_clear()


@pytest.mark.network
def test_country_geometry_resolves_nepal():
    geom = country_geometry("nepal")
    assert isinstance(geom, (Polygon, MultiPolygon))
    minx, miny, maxx, maxy = geom.bounds
    assert 80 < minx < 90 and 26 < miny < 31
    assert 86 < maxx < 90 and 28 < maxy < 32


@pytest.mark.network
def test_country_geometry_unknown_region():
    with pytest.raises(UnknownRegionError):
        country_geometry("notarealcountry")


# --- load_boundary: region-name resolution ---


@pytest.mark.network
def test_load_boundary_accepts_region_name():
    geom = load_boundary("nepal")
    assert isinstance(geom, (Polygon, MultiPolygon))
    minx, miny, maxx, maxy = geom.bounds
    assert 80 < minx < 90 and 26 < miny < 31


@pytest.mark.network
def test_load_boundary_unknown_name_raises():
    with pytest.raises(ValueError, match="not valid JSON"):
        load_boundary("notarealcountry")


def test_load_boundary_accepts_geojson_file(tmp_path):
    feat = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        },
        "properties": {},
    }
    p = tmp_path / "boundary.geojson"
    p.write_text(json.dumps(feat))
    geom = load_boundary(str(p))
    assert isinstance(geom, Polygon)


def test_load_boundary_accepts_inline_geojson():
    inline = json.dumps(
        {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        }
    )
    geom = load_boundary(inline)
    assert isinstance(geom, Polygon)
