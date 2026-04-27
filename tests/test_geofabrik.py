"""Integration test against the live Geofabrik index."""

from __future__ import annotations

import pytest

from osmsg.geofabrik import UnknownRegionError, country_update_url, load_index


@pytest.mark.network
def test_country_update_url_resolves_nepal():
    url = country_update_url("nepal")
    assert url.startswith("https://download.geofabrik.de/")
    assert url.endswith("nepal-updates")


@pytest.mark.network
def test_country_update_url_unknown_region():
    with pytest.raises(UnknownRegionError):
        country_update_url("notarealcountry")


@pytest.mark.network
def test_load_index_caches_in_memory():
    a = load_index()
    b = load_index()
    assert a is b
