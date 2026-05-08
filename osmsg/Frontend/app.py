from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from litestar import Litestar, get
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.plugins.htmx import HTMXPlugin, HTMXRequest
from litestar.response import Template
from litestar.static_files import StaticFilesConfig
from litestar.template.config import TemplateConfig

API_BASE = "https://osmsg-1.onrender.com/api/v1/user-stats"

BASE_DIR = Path(__file__).parent


def _current_year() -> int:
    return datetime.utcnow().year


def _parse_dates(daterange_str: str) -> tuple[str, str]:
    now = datetime.utcnow()
    yesterday = now - timedelta(days=1)

    fallback = (
        yesterday.strftime("%Y-%m-%dT00:00:00Z"),
        now.strftime("%Y-%m-%dT23:59:59Z"),
    )

    if not daterange_str:
        return fallback

    try:
        if "to" in daterange_str:
            left, right = daterange_str.split("to", 1)

            d1 = datetime.strptime(left.strip(), "%d-%m-%Y")
            d2 = datetime.strptime(right.strip(), "%d-%m-%Y")
        else:
            d1 = d2 = datetime.strptime(
                daterange_str.strip(),
                "%d-%m-%Y",
            )

        return (
            d1.strftime("%Y-%m-%dT00:00:00Z"),
            d2.strftime("%Y-%m-%dT23:59:59Z"),
        )

    except (ValueError, AttributeError):
        return fallback


def _fetch(params: dict) -> dict:
    url = f"{API_BASE}?{urllib.parse.urlencode(params, doseq=True)}"

    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json"},
    )

    with urllib.request.urlopen(req, timeout=90) as response:
        raw = response.read().decode()

        if not raw.strip():
            return {}

        return json.loads(raw)


def _url_for(endpoint: str, **values: str) -> str:
    if endpoint == "static":
        filename = values.get("filename", "")
        return f"/static/{filename}"

    return f"/{endpoint}"


@get("/")
async def index(request: HTMXRequest) -> Template:
    return Template(
        "index.html",
        context={
            "current_year": _current_year(),
        },
    )


@get("/statistics")
async def statistics(
    request: HTMXRequest,
    hashtags: list[str] | None = None,
    daterange: str = "",
    limit: int = 25,
    offset: int = 0,
) -> Template:

    start, end = _parse_dates(daterange)

    params = {
        "start": start,
        "end": end,
        "limit": limit,
        "offset": offset,
    }

    if hashtags:
        params["hashtag"] = ",".join(
            f"#{tag.lstrip('#')}"
            for tag in hashtags
        )

    raw = _fetch(params)

    print(params)
    print(raw)

    users = raw.get("users", [])

    meta = {
        "start": raw.get("start", start),
        "end": raw.get("end", end),
        "count": raw.get("count", len(users)),
        "hashtag": raw.get("hashtag", hashtags or []),
        "limit": raw.get("limit", limit),
        "offset": raw.get("offset", offset),
    }

    template = (
        "partials/leaderboard.html"
        if request.htmx
        else "statistics.html"
    )

    return Template(
        template,
        context={
            "users": users,
            "meta": meta,
            "hashtags": hashtags or [],
            "daterange": daterange,
            "limit": limit,
            "offset": offset,
            "current_year": _current_year(),
        },
    )


@get("/api/proxy")
async def api_proxy(
    request: HTMXRequest,
    hashtags: list[str] | None = None,
    daterange: str = "",
    limit: int = 25,
    offset: int = 0,
) -> dict:

    start, end = _parse_dates(daterange)

    params = {
        "start": start,
        "end": end,
        "limit": limit,
        "offset": offset,
    }

    if hashtags:
        params["hashtag"] = ",".join(
            f"#{tag.lstrip('#')}"
            for tag in hashtags
        )

    return _fetch(params)


@get("/about")
async def about(request: HTMXRequest) -> Template:
    return Template(
        "about.html",
        context={
            "current_year": _current_year(),
        },
    )


@get("/contact")
async def contact(request: HTMXRequest) -> Template:
    return Template(
        "contact.html",
        context={
            "current_year": _current_year(),
        },
    )


def _configure_jinja(engine: JinjaTemplateEngine) -> None:
    engine.engine.globals["url_for"] = _url_for


app = Litestar(
    route_handlers=[
        index,
        statistics,
        api_proxy,
        about,
        contact,
    ],
    plugins=[HTMXPlugin()],
    template_config=TemplateConfig(
        directory=BASE_DIR / "templates",
        engine=JinjaTemplateEngine,
        engine_callback=_configure_jinja,
    ),
    static_files_config=[
        StaticFilesConfig(
            directories=[BASE_DIR / "static"],
            path="/static",
            name="static",
        )
    ],
    debug=True,
)