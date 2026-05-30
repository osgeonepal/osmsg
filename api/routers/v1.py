from datetime import UTC, datetime, timedelta
from typing import Annotated

from litestar import Controller, Router, get
from litestar.exceptions import HTTPException
from litestar.params import Parameter

from ..queries import fetch_editor_stats, fetch_hashtag_stats, fetch_hashtag_trends, fetch_user_stats
from ..schemas import (
    EditorStat,
    EditorStatsResponse,
    HashtagStat,
    HashtagStatsResponse,
    HashtagTrend,
    UserStat,
    UserStatsResponse,
)

TREND_INTERVALS = {"day", "week", "month"}


def normalize_hashtags(hashtag: list[str] | None) -> list[str] | None:
    if not hashtag:
        return None

    normalized: list[str] = []
    seen: set[str] = set()
    for value in hashtag:
        cleaned = value.strip()
        if not cleaned:
            continue
        cleaned = "#" + cleaned.lstrip("#")
        key = cleaned.lower()
        if key not in seen:
            normalized.append(cleaned)
            seen.add(key)
    return normalized or None


def resolve_optional_window(start: datetime | None, end: datetime | None) -> tuple[datetime | None, datetime | None]:
    start = start or (datetime.min.replace(tzinfo=UTC) if end else None)
    end = end or (datetime.now(tz=UTC) if start else None)
    if start and end and start >= end:
        raise HTTPException(status_code=400, detail="start must be before end")
    return start, end


def resolve_required_window(start: datetime | None, end: datetime | None) -> tuple[datetime, datetime]:
    end = end or datetime.now(tz=UTC)
    start = start or (end - timedelta(days=30))
    if start >= end:
        raise HTTPException(status_code=400, detail="start must be before end")
    return start, end


class StatsController(Controller):
    path = "/stats"

    @get()
    async def get_user_stats(
        self,
        start: Annotated[
            datetime | None, Parameter(description="Inclusive UTC lower bound (ISO 8601). If omitted, no lower bound.")
        ] = None,
        end: Annotated[
            datetime | None,
            Parameter(description="Exclusive UTC upper bound (ISO 8601). Defaults to now if start is set."),
        ] = None,
        hashtag: Annotated[
            list[str] | None, Parameter(description="Filter to changesets carrying any of these hashtags. Repeatable.")
        ] = None,
        tags: Annotated[bool, Parameter(description="Include per-user tag_stats breakdown in the response.")] = True,
        limit: Annotated[int, Parameter(ge=1, le=1000, description="Page size (1–1000).")] = 100,
        offset: Annotated[int, Parameter(ge=0, description="Page offset.")] = 0,
    ) -> UserStatsResponse:
        start, end = resolve_optional_window(start, end)
        normalized_hashtag = normalize_hashtags(hashtag)
        rows = await fetch_user_stats(
            start=start,
            end=end,
            hashtag=normalized_hashtag,
            tags=tags,
            limit=limit,
            offset=offset,
        )
        users = [UserStat(**row) for row in rows]
        return UserStatsResponse(
            count=len(users),
            start=start,
            end=end,
            hashtag=normalized_hashtag,
            tags=tags,
            limit=limit,
            offset=offset,
            users=users,
        )


class HashtagStatsController(Controller):
    path = "/hashtag-stats"

    @get()
    async def get_hashtag_stats(
        self,
        start: Annotated[
            datetime | None,
            Parameter(description="Inclusive UTC lower bound (ISO 8601). Defaults to 30 days before end."),
        ] = None,
        end: Annotated[
            datetime | None,
            Parameter(description="Exclusive UTC upper bound (ISO 8601). Defaults to now."),
        ] = None,
        hashtag: Annotated[
            list[str] | None, Parameter(description="Optional hashtags to limit the leaderboard to. Repeatable.")
        ] = None,
        interval: Annotated[str, Parameter(description="Trend bucket: day, week, or month.")] = "day",
        limit: Annotated[int, Parameter(ge=1, le=1000, description="Page size (1-1000).")] = 100,
        offset: Annotated[int, Parameter(ge=0, description="Page offset.")] = 0,
    ) -> HashtagStatsResponse:
        if interval not in TREND_INTERVALS:
            raise HTTPException(status_code=400, detail="interval must be one of: day, week, month")

        start, end = resolve_required_window(start, end)
        normalized_hashtag = normalize_hashtags(hashtag)
        hashtag_rows = await fetch_hashtag_stats(
            start=start,
            end=end,
            hashtag=normalized_hashtag,
            limit=limit,
            offset=offset,
        )
        trend_rows = await fetch_hashtag_trends(
            start=start,
            end=end,
            interval=interval,
            hashtag=normalized_hashtag,
            limit=limit,
            offset=offset,
        )
        hashtags = [HashtagStat(**row) for row in hashtag_rows]
        trends = [HashtagTrend(**row) for row in trend_rows]
        return HashtagStatsResponse(
            count=len(hashtags),
            start=start,
            end=end,
            hashtag=normalized_hashtag,
            interval=interval,
            limit=limit,
            offset=offset,
            hashtags=hashtags,
            trends=trends,
        )


class EditorStatsController(Controller):
    path = "/editor-stats"

    @get()
    async def get_editor_stats(
        self,
        start: Annotated[
            datetime | None, Parameter(description="Inclusive UTC lower bound (ISO 8601). If omitted, no lower bound.")
        ] = None,
        end: Annotated[
            datetime | None,
            Parameter(description="Exclusive UTC upper bound (ISO 8601). Defaults to now if start is set."),
        ] = None,
        limit: Annotated[int, Parameter(ge=1, le=1000, description="Page size (1-1000).")] = 100,
        offset: Annotated[int, Parameter(ge=0, description="Page offset.")] = 0,
    ) -> EditorStatsResponse:
        start, end = resolve_optional_window(start, end)
        rows = await fetch_editor_stats(
            start=start,
            end=end,
            limit=limit,
            offset=offset,
        )
        editors = [EditorStat(**row) for row in rows]
        return EditorStatsResponse(
            count=len(editors),
            start=start,
            end=end,
            limit=limit,
            offset=offset,
            editors=editors,
        )


v1_router = Router(
    path="/api/v1",
    route_handlers=[StatsController, HashtagStatsController, EditorStatsController],
)
