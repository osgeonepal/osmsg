"""Shared helpers for the v2 routers."""

from datetime import UTC, datetime

from litestar.exceptions import HTTPException

from osmsg.query import LEADERBOARD_SORTS


def to_utc(value: datetime | None) -> datetime | None:
    """Normalize a query-parameter datetime to UTC; Litestar parses an offset-less ISO string as naive,
    which is taken as UTC so the window is not shifted by the server's local offset."""
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def validate_page(sort: str, order: str, page: int) -> None:
    """Reject bad leaderboard paging with a 400 before the query reaches a worker thread."""
    if sort not in LEADERBOARD_SORTS:
        raise HTTPException(status_code=400, detail=f"sort must be one of {', '.join(LEADERBOARD_SORTS)}")
    if order not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")
    if page < 1:
        raise HTTPException(status_code=400, detail="page must be >= 1")
