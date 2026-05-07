from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from litestar import Litestar, get
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.exceptions import HTTPException
from litestar.openapi.config import OpenAPIConfig
from litestar.params import Parameter
from litestar.response import Template
from litestar.template.config import TemplateConfig

from .db import close_pool, ensure_schema, open_pool
from .queries import fetch_state, fetch_user_stats

TEMPLATES = Path(__file__).parent / "templates"


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


@get("/", include_in_schema=False)
async def home() -> Template:
    return Template("home.html")


@get("/health")
async def health() -> dict[str, Any]:
    state = await fetch_state()
    return {
        "status": "ok",
        "last_seq": state["last_seq"] if state else None,
        "last_updated": state["last_ts"].isoformat() if state else None,
    }


@get("/api/v1/user-stats")
async def get_user_stats(
    start: datetime | None = None,
    end: datetime | None = None,
    hashtag: list[str] | None = None,
    limit: int = Parameter(default=100, ge=1, le=1000),
    offset: int = Parameter(default=0, ge=0),
) -> dict[str, Any]:
    start = start or (datetime.min.replace(tzinfo=UTC) if end else None)
    end = end or (datetime.now(tz=UTC) if start else None)
    if start and end and start >= end:
        raise HTTPException(status_code=400, detail="start must be before end")

    normalized_hashtag = normalize_hashtags(hashtag)
    users = await fetch_user_stats(start=start, end=end, hashtag=normalized_hashtag, limit=limit, offset=offset)
    return {
        "count": len(users),
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "hashtag": normalized_hashtag,
        "limit": limit,
        "offset": offset,
        "users": users,
    }


app = Litestar(
    route_handlers=[home, health, get_user_stats],
    lifespan=[lifespan],
    openapi_config=OpenAPIConfig(title="OSMSG API", version="1.0.0", path="/docs"),
    template_config=TemplateConfig(directory=TEMPLATES, engine=JinjaTemplateEngine),  # ty: ignore[invalid-argument-type]
)
