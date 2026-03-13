from __future__ import annotations

from pydantic import BaseModel, Field


class ChangesetMeta(BaseModel):
    """Metadata extracted from changeset replication files."""

    hashtags: list[str] = Field(default_factory=list)
    countries: list[str] = Field(default_factory=list)
    editors: list[str] = Field(default_factory=list)


class ActionCounts(BaseModel):
    """Create/modify/delete counts for a single OSM element type."""

    create: int = 0
    modify: int = 0
    delete: int = 0

    def increment(self, action: str) -> None:
        setattr(self, action, getattr(self, action) + 1)


class POIActionCounts(BaseModel):
    """Create/modify counts for POI and user-supplied tags (no delete tracking)."""

    create: int = 0
    modify: int = 0

    def increment(self, action: str) -> None:
        setattr(self, action, getattr(self, action) + 1)


class UserRecord(BaseModel):
    """Per-user contribution stats collected during changefile processing."""

    name: str
    uid: int
    changesets: int = 0

    # OSM element counts
    nodes: ActionCounts = Field(default_factory=ActionCounts)
    ways: ActionCounts = Field(default_factory=ActionCounts)
    relations: ActionCounts = Field(default_factory=ActionCounts)
    poi: POIActionCounts = Field(default_factory=POIActionCounts)

    # populated when --changeset or --hashtags is active
    countries: list[str] = Field(default_factory=list)
    hashtags: list[str] = Field(default_factory=list)
    editors: list[str] = Field(default_factory=list)

    # populated when --all_tags is active
    tags_create: dict[str, int] = Field(default_factory=dict)
    tags_modify: dict[str, int] = Field(default_factory=dict)

    # populated when --tags is active
    additional_tag_stats: dict[str, POIActionCounts] = Field(default_factory=dict)

    # populated when --length is active
    lengths: dict[str, float] = Field(default_factory=dict)


class UsersTemp(BaseModel):
    """Temporary per-user state used for changeset deduplication during processing."""

    changesets: list[int] = Field(default_factory=list)


class SummaryInterval(BaseModel):
    """Aggregate stats per date interval, populated when --summary is active."""

    timestamp: str
    users: int = 0
    changesets: int = 0

    # OSM element counts
    nodes: ActionCounts = Field(default_factory=ActionCounts)
    ways: ActionCounts = Field(default_factory=ActionCounts)
    relations: ActionCounts = Field(default_factory=ActionCounts)
    poi: POIActionCounts = Field(default_factory=POIActionCounts)

    # populated when --changeset or --hashtags is active
    editors: dict[str, int] = Field(default_factory=dict)

    # populated when --all_tags is active
    tags_create: dict[str, int] = Field(default_factory=dict)
    tags_modify: dict[str, int] = Field(default_factory=dict)

    # populated when --tags is active
    additional_tag_stats: dict[str, POIActionCounts] = Field(default_factory=dict)

    # populated when --length is active
    lengths: dict[str, float] = Field(default_factory=dict)


class SummaryIntervalTemp(BaseModel):
    """Temporary per-date state used for changeset and user deduplication during summary processing."""

    changesets: list[int] = Field(default_factory=list)
    users: list[int] = Field(default_factory=list)
