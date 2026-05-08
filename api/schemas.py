from datetime import datetime

from pydantic import BaseModel


class TagValueStats(BaseModel):
    c: int = 0
    m: int = 0
    len: float | None = None


class UserStat(BaseModel):
    uid: int
    name: str
    changesets: int
    nodes_create: int
    nodes_modify: int
    nodes_delete: int
    ways_create: int
    ways_modify: int
    ways_delete: int
    rels_create: int
    rels_modify: int
    rels_delete: int
    poi_create: int
    poi_modify: int
    map_changes: int
    rank: int
    tag_stats: dict[str, dict[str, TagValueStats]] | None = None


class UserStatsResponse(BaseModel):
    count: int
    start: datetime | None
    end: datetime | None
    hashtag: list[str] | None
    tags: bool
    limit: int
    offset: int
    users: list[UserStat]


class HealthResponse(BaseModel):
    status: str
    last_seq: int | None
    last_ts: datetime | None
    updated_at: datetime | None
