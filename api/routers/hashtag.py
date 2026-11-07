"""Hashtag endpoints. The path hashtag is a comma-separated list of prefixes whose union is deduped by
changeset; an optional half-open [start, end) window narrows any endpoint."""

from datetime import datetime
from typing import Any

from litestar import Controller, Router, get
from litestar.exceptions import HTTPException

from osmsg.query import TREND_INTERVALS as _TREND_INTERVALS

from .. import duck
from ._common import to_utc, validate_page


def _hashtags(hashtag: str) -> list[str]:
    """Split the comma-separated path segment into one or more non-empty hashtags."""
    tags = [h.strip() for h in hashtag.split(",") if h.strip()]
    if not tags:
        raise HTTPException(status_code=400, detail="at least one hashtag is required")
    return tags


def _window(start: datetime | None, end: datetime | None) -> tuple[datetime | None, datetime | None]:
    """Validate an optional [start, end) window, normalizing each bound to UTC. Either bound may be
    omitted; an inverted range is a client error, not silently empty results."""
    start, end = to_utc(start), to_utc(end)
    if start is not None and end is not None and start >= end:
        raise HTTPException(status_code=400, detail="start must be before end")
    return start, end


class HashtagController(Controller):
    path = "/hashtag/{hashtag:str}"

    @get("/summary")
    async def get_summary(
        self, hashtag: str, exact: bool = False, start: datetime | None = None, end: datetime | None = None
    ) -> dict[str, Any]:
        start, end = _window(start, end)
        return await duck.summary(_hashtags(hashtag), exact=exact, start=start, end=end)

    @get("/leaderboard")
    async def get_leaderboard(
        self,
        hashtag: str,
        exact: bool = False,
        page: int = 1,
        page_size: int = 25,
        sort: str = "map_changes",
        order: str = "desc",
        q: str | None = None,
        detail: bool = False,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, Any]:
        """One page of the leaderboard, sorted, searched and paged server-side; per-user detail only with
        `detail=true`."""
        validate_page(sort, order, page)
        start, end = _window(start, end)
        return await duck.leaderboard(
            _hashtags(hashtag),
            exact=exact,
            page=page,
            page_size=page_size,
            sort=sort,
            order=order,
            q=q,
            detail=detail,
            start=start,
            end=end,
        )

    @get("/user/{uid:int}")
    async def get_user(
        self,
        hashtag: str,
        uid: int,
        exact: bool = False,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, Any]:
        """One contributor's detail (tag_stats, editors, hashtags) for the scope, fetched on demand when a
        profile opens. uid-scoped, so it stays fast even on an all-time mega-hashtag."""
        start, end = _window(start, end)
        return await duck.user_detail(uid, hashtag=_hashtags(hashtag), exact=exact, start=start, end=end)

    @get("/tags")
    async def get_tags(
        self,
        hashtag: str,
        exact: bool = False,
        limit: int = 100,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, Any]]:
        start, end = _window(start, end)
        return await duck.tags(_hashtags(hashtag), exact=exact, limit=limit, start=start, end=end)

    @get("/editors")
    async def get_editors(
        self, hashtag: str, exact: bool = False, start: datetime | None = None, end: datetime | None = None
    ) -> list[dict[str, Any]]:
        start, end = _window(start, end)
        return await duck.editors(_hashtags(hashtag), exact=exact, start=start, end=end)

    @get("/hashtags")
    async def get_hashtags(
        self,
        hashtag: str,
        exact: bool = False,
        limit: int = 15,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Trending hashtags in the queried scope, ranked by distinct contributors: `[{hashtag, users, edits}]`."""
        start, end = _window(start, end)
        return await duck.hashtags(_hashtags(hashtag), exact=exact, limit=limit, start=start, end=end)

    @get("/trends")
    async def get_trends(
        self,
        hashtag: str,
        exact: bool = False,
        interval: str = "day",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if interval not in _TREND_INTERVALS:
            raise HTTPException(status_code=400, detail=f"interval must be one of {', '.join(_TREND_INTERVALS)}")
        start, end = _window(start, end)
        return await duck.trends(_hashtags(hashtag), exact=exact, interval=interval, start=start, end=end)

    @get("/map")
    async def get_map(
        self,
        hashtag: str,
        exact: bool = False,
        limit: int = 2000,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, Any]:
        """A GeoJSON FeatureCollection of changeset centroids (Point), for clustering and heatmaps."""
        start, end = _window(start, end)
        points = await duck.map_points(_hashtags(hashtag), exact=exact, limit=limit, start=start, end=end)
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [p["lon"], p["lat"]]},
                    "properties": {"changeset_id": p["changeset_id"], "uid": p["uid"]},
                }
                for p in points
            ],
        }


v2_router = Router(path="/api/v2", route_handlers=[HashtagController])
