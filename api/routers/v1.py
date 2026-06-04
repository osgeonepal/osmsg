from datetime import UTC, datetime
from typing import Annotated

from litestar import Controller, Router, get
from litestar.exceptions import HTTPException
from litestar.params import Parameter

from ..queries import fetch_user_stats
from ..schemas import UserStat, UserStatsResponse


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
        start = start or (datetime.min.replace(tzinfo=UTC) if end else None)
        end = end or (datetime.now(tz=UTC) if start else None)
        if start and end and start >= end:
            raise HTTPException(status_code=400, detail="start must be before end")

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


v1_router = Router(path="/api/v1", route_handlers=[StatsController])
