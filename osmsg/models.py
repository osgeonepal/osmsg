from __future__ import annotations

from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class Action(Enum):
    CREATE = "create"
    MODIFY = "modify"
    DELETE = "delete"


class User(BaseModel):
    uid: int
    username: str


class Changeset(BaseModel):
    """Metadata extracted from changeset replication files."""

    changeset_id: int
    uid: int
    created_at: datetime | None = None
    hashtags: list[str] = Field(default_factory=list)
    editor: str | None = None
    bbox: tuple[float, float, float, float] | None = Field(default=None, description="(min_lon, min_lat, max_lon, max_lat)")


class ElementStat(BaseModel):
    """Create/modify/delete counts for a single OSM element type."""

    c: int = 0
    m: int = 0
    d: int = 0

    @property
    def total(self) -> int:
        return self.c + self.m + self.d

    def add(self, action: Action) -> None:
        """Increment the appropriate counter based on action type."""
        if action == Action.CREATE.value:
            self.c += 1
        elif action == Action.MODIFY.value:
            self.m += 1
        elif action == Action.DELETE.value:
            self.d += 1


class TagValueStat(BaseModel):
    c: int = 0
    m: int = 0
    len: float | None = None

    def add_action(self, action: Action) -> None:
        if action == Action.CREATE.value:
            self.c += 1
        elif action == Action.MODIFY.value:
            self.m += 1

    def add_length(self, length: float) -> None:
        if self.len is None:
            self.len = 0.0
        self.len += length

    @property
    def to_flat_dict(self) -> dict:
        """
        Serialise to a plain dict for JSON storage.
        """
        d: dict = {"c": self.c, "m": self.m}
        if self.len is not None:
            d["len"] = round(self.len, 2)
        return d


class ChangesetStats(BaseModel):
    changeset_id: int
    uid: int

    nodes: ElementStat = Field(default_factory=ElementStat)
    ways: ElementStat = Field(default_factory=ElementStat)
    rels: ElementStat = Field(default_factory=ElementStat)

    poi_created: int = 0
    poi_modified: int = 0

    tag_stats: dict[str, dict[str, TagValueStat]] = Field(default_factory=dict)

    @property
    def map_changes(self) -> int:
        return self.nodes.total + self.ways.total + self.rels.total

    @property
    def tag_stats_as_dict(self) -> dict:
        """
        Convert nested TagValueStat objects to plain dicts for JSON serialisation.

        Example output:
          {
            "building": {"yes": {"c": 35, "m": 2}},
            "highway":  {"residential": {"c": 8, "m": 1, "len": 2400.0}}
          }
        """
        return {
            tag_key: {tag_val: stat.to_flat_dict for tag_val, stat in val_dict.items()}
            for tag_key, val_dict in self.tag_stats.items()
        }

    @property
    def to_dict(self) -> dict:
        return {
            "nodes_created": self.nodes.c,
            "nodes_modified": self.nodes.m,
            "nodes_deleted": self.nodes.d,
            "ways_created": self.ways.c,
            "ways_modified": self.ways.m,
            "ways_deleted": self.ways.d,
            "rels_created": self.rels.c,
            "rels_modified": self.rels.m,
            "rels_deleted": self.rels.d,
            "poi_created": self.poi_created,
            "poi_modified": self.poi_modified,
            "tag_stats": self.tag_stats_as_dict,
        }
