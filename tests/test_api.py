from importlib import import_module

from litestar import Litestar
from litestar.testing import TestClient

from api import app as api_app
from api.app import health
from api.pg_schema import PG_SCHEMA as API_PG_SCHEMA
from api.routers.v1 import normalize_hashtags, v1_router
from osmsg.pg_schema import PG_SCHEMA as CLI_PG_SCHEMA

v1_module = import_module("api.routers.v1")


def test_pg_schema_in_sync():
    assert API_PG_SCHEMA == CLI_PG_SCHEMA


def test_api_exposes_only_active_public_routes():
    paths = {route.path for route in api_app.routes}

    assert "/health" in paths
    assert "/api/v1/stats" in paths
    assert "/api/v1/stats/summary" not in paths
    assert "/api/v1/stats/timeseries" not in paths


def test_health_endpoint_returns_ok():
    with TestClient(Litestar(route_handlers=[health])) as client:
        response = client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["last_seq"] is None
    assert data["last_updated"] is None


def test_normalize_hashtags_accepts_bare_or_prefixed_values():
    assert normalize_hashtags(["maproulette", "#HOTOSM", "  #roads  ", ""]) == [
        "#maproulette",
        "#HOTOSM",
        "#roads",
    ]


def test_normalize_hashtags_dedupes_case_insensitively():
    assert normalize_hashtags(["maproulette", "#MapRoulette", "#roads"]) == ["#maproulette", "#roads"]


def _stats_app(monkeypatch, fake_fetch):
    monkeypatch.setattr(v1_module, "fetch_user_stats", fake_fetch)
    return Litestar(route_handlers=[v1_router])


def test_user_stats_endpoint_returns_expected_response(monkeypatch):
    async def fake_fetch_user_stats(*, start, end, hashtag, tags, limit, offset):
        assert start.isoformat() == "2026-05-01T00:00:00+00:00"
        assert end.isoformat() == "2026-05-02T00:00:00+00:00"
        assert hashtag == ["#mapathon", "#roads"]
        assert tags is True
        assert limit == 1
        assert offset == 0
        return [
            {
                "uid": 10,
                "name": "alice",
                "changesets": 2,
                "nodes_create": 40,
                "nodes_modify": 5,
                "nodes_delete": 0,
                "ways_create": 12,
                "ways_modify": 1,
                "ways_delete": 0,
                "rels_create": 0,
                "rels_modify": 0,
                "rels_delete": 0,
                "poi_create": 5,
                "poi_modify": 1,
                "map_changes": 58,
                "rank": 1,
                "tag_stats": {"building": {"yes": {"c": 3, "m": 0}}},
            }
        ]

    with TestClient(_stats_app(monkeypatch, fake_fetch_user_stats)) as client:
        response = client.get(
            "/api/v1/stats",
            params=[
                ("start", "2026-05-01T00:00:00Z"),
                ("end", "2026-05-02T00:00:00Z"),
                ("hashtag", "mapathon"),
                ("hashtag", "#roads"),
                ("limit", "1"),
            ],
        )

    assert response.status_code == 200
    assert response.json() == {
        "count": 1,
        "start": "2026-05-01T00:00:00Z",
        "end": "2026-05-02T00:00:00Z",
        "hashtag": ["#mapathon", "#roads"],
        "tags": True,
        "limit": 1,
        "offset": 0,
        "users": [
            {
                "uid": 10,
                "name": "alice",
                "changesets": 2,
                "nodes_create": 40,
                "nodes_modify": 5,
                "nodes_delete": 0,
                "ways_create": 12,
                "ways_modify": 1,
                "ways_delete": 0,
                "rels_create": 0,
                "rels_modify": 0,
                "rels_delete": 0,
                "poi_create": 5,
                "poi_modify": 1,
                "map_changes": 58,
                "rank": 1,
                "tag_stats": {"building": {"yes": {"c": 3, "m": 0, "len": None}}},
            }
        ],
    }


def test_user_stats_endpoint_rejects_invalid_date_range(monkeypatch):
    async def fake_fetch_user_stats(**kwargs):
        raise AssertionError("fetch_user_stats should not be called")

    with TestClient(_stats_app(monkeypatch, fake_fetch_user_stats)) as client:
        response = client.get(
            "/api/v1/stats",
            params={"start": "2026-05-02T00:00:00Z", "end": "2026-05-01T00:00:00Z"},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "start must be before end"


def test_user_stats_endpoint_tags_false_drops_tag_stats(monkeypatch):
    async def fake_fetch_user_stats(*, tags, **_kwargs):
        assert tags is False
        return [
            {
                "uid": 10,
                "name": "alice",
                "changesets": 1,
                "nodes_create": 0,
                "nodes_modify": 0,
                "nodes_delete": 0,
                "ways_create": 0,
                "ways_modify": 0,
                "ways_delete": 0,
                "rels_create": 0,
                "rels_modify": 0,
                "rels_delete": 0,
                "poi_create": 0,
                "poi_modify": 0,
                "map_changes": 0,
                "rank": 1,
                "tag_stats": None,
            }
        ]

    with TestClient(_stats_app(monkeypatch, fake_fetch_user_stats)) as client:
        response = client.get("/api/v1/stats", params={"tags": "false"})

    assert response.status_code == 200
    body = response.json()
    assert body["tags"] is False
    assert body["users"][0]["tag_stats"] is None


def test_user_stats_sql_omits_tag_ctes_when_tags_false():
    from api.queries import _user_stats_sql

    sql_with = _user_stats_sql(filter_dates=False, filter_hashtags=False, include_tags=True)
    sql_without = _user_stats_sql(filter_dates=False, filter_hashtags=False, include_tags=False)
    assert "tag_per_user" in sql_with
    assert "tag_per_user" not in sql_without
    assert "NULL::jsonb AS tag_stats" in sql_without
