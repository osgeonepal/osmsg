from __future__ import annotations

from osmsg.api.app import app


def test_api_exposes_only_active_public_routes():
    paths = {route.path for route in app.routes}

    assert "/health" in paths
    assert "/api/v1/users" in paths
    assert "/api/v1/stats/summary" not in paths
    assert "/api/v1/stats/timeseries" not in paths
