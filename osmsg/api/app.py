from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from litestar import Litestar, get
from litestar.openapi.config import OpenAPIConfig
from litestar.params import Parameter

from .db import close_pool, open_pool
from .queries import fetch_users


@asynccontextmanager
async def lifespan(app: Litestar):
    await open_pool()
    try:
        yield
    finally:
        await close_pool()


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@get("/api/v1/users")
async def get_users(
    limit: int = Parameter(default=100, ge=1, le=1000),
    offset: int = Parameter(default=0, ge=0),
) -> dict[str, Any]:
    users = await fetch_users(limit=limit, offset=offset)
    return {"count": len(users), "limit": limit, "offset": offset, "users": users}


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
    route_handlers=[health, get_users],
    lifespan=[lifespan],
    openapi_config=OpenAPIConfig(title="OSMSG API", version="1.0.0", path="/docs"),
)
