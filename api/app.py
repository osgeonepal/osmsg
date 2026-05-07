from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from litestar import Litestar, get
from litestar.exceptions import HTTPException
from litestar.openapi.config import OpenAPIConfig
from litestar.params import Parameter

from .db import close_pool, ensure_schema, open_pool
from .queries import fetch_user_stats


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


@asynccontextmanager
async def lifespan(app: Litestar):
    await open_pool()
    await ensure_schema()
    try:
        yield
    finally:
        await close_pool()


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@get("/api/v1/user-stats")
async def get_user_stats(
    start: datetime,
    end: datetime,
    hashtag: list[str] | None = None,
    limit: int = Parameter(default=100, ge=1, le=1000),
    offset: int = Parameter(default=0, ge=0),
) -> dict[str, Any]:
    if start >= end:
        raise HTTPException(status_code=400, detail="start must be before end")

    normalized_hashtag = normalize_hashtags(hashtag)
    users = await fetch_user_stats(start=start, end=end, hashtag=normalized_hashtag, limit=limit, offset=offset)
    return {
        "count": len(users),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "hashtag": normalized_hashtag,
        "limit": limit,
        "offset": offset,
        "users": users,
    }


# @get("/api/v1/stats/summary")
# async def get_summary(start_date: datetime, end_date: datetime, hashtag: str | None = None) -> dict:
#     if start_date > end_date:
#         return {"error": "start_date must be before end_date"}
#     return {"message": "Temporarily disabled"}


# @get("/api/v1/stats/timeseries")
# async def get_timeseries(start_date: datetime, end_date: datetime, hashtag: str | None = None) -> dict:
#     if start_date > end_date:
#         return {"error": "start_date must be before end_date"}
#     return {"message": "Temporarily disabled"}


app = Litestar(
    route_handlers=[health, get_user_stats],
    lifespan=[lifespan],
    openapi_config=OpenAPIConfig(title="OSMSG API", version="1.0.0", path="/docs"),
)
