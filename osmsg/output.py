from __future__ import annotations

import json

from .models import (
    SummaryInterval,
    UserRecord,
)


def users_export_dict(record: UserRecord) -> dict:
    """Convert a UserRecord into a flat dict suitable for DataFrame construction."""
    from .app import (
        additional_tags,
        all_tags,
        changeset_meta,
        hashtags,
        length,
    )

    exclude_fields = {
        "nodes",
        "ways",
        "relations",
        "poi",
        "additional_tag_stats",
        "lengths",
    }

    if not (hashtags or changeset_meta):
        exclude_fields.update({"countries", "hashtags", "editors"})

    if not all_tags:
        exclude_fields.update({"tags_create", "tags_modify"})

    base = record.model_dump(exclude=exclude_fields)

    # Flatten nodes/ways/relations/poi
    for element in ("nodes", "ways", "relations"):
        counts = getattr(record, element)
        base[f"{element}.create"] = counts.create
        base[f"{element}.modify"] = counts.modify
        base[f"{element}.delete"] = counts.delete

    base["poi.create"] = record.poi.create
    base["poi.modify"] = record.poi.modify

    # serialize tags dicts to JSON string
    if all_tags:
        base["tags_create"] = json.dumps(dict(sorted(record.tags_create.items(), key=lambda item: item[1], reverse=True)))
        base["tags_modify"] = json.dumps(dict(sorted(record.tags_modify.items(), key=lambda item: item[1], reverse=True)))

    # flatten additional_tag_stats
    if additional_tags and record.additional_tag_stats:
        for tag, counts in record.additional_tag_stats.items():
            base[f"{tag}.create"] = counts.create
            base[f"{tag}.modify"] = counts.modify

    # flatten lengths
    if length and record.lengths:
        for tag, value in record.lengths.items():
            base[f"{tag}_len_m"] = value
    return base


def summary_export_dict(record: SummaryInterval) -> dict:
    """Convert a SummaryInterval into a flat dict suitable for DataFrame construction."""
    from .app import (
        additional_tags,
        all_tags,
        changeset_meta,
        hashtags,
        length,
    )

    base = {
        "timestamp": record.timestamp,
        "users": record.users,
        "changesets": record.changesets,
        "nodes.create": record.nodes.create,
        "nodes.modify": record.nodes.modify,
        "nodes.delete": record.nodes.delete,
        "ways.create": record.ways.create,
        "ways.modify": record.ways.modify,
        "ways.delete": record.ways.delete,
        "relations.create": record.relations.create,
        "relations.modify": record.relations.modify,
        "relations.delete": record.relations.delete,
        "poi.create": record.poi.create,
        "poi.modify": record.poi.modify,
    }

    # editors as JSON string
    if hashtags or changeset_meta:
        base["editors"] = json.dumps(dict(sorted(record.editors.items(), key=lambda item: item[1], reverse=True)))

    # tags as JSON string
    if all_tags:
        base["tags_create"] = json.dumps(dict(sorted(record.tags_create.items(), key=lambda item: item[1], reverse=True)))
        base["tags_modify"] = json.dumps(dict(sorted(record.tags_modify.items(), key=lambda item: item[1], reverse=True)))

    # flatten additional_tag_stats
    if additional_tags and record.additional_tag_stats:
        for tag, counts in record.additional_tag_stats.items():
            base[f"{tag}.create"] = counts.create
            base[f"{tag}.modify"] = counts.modify

    # flatten lengths
    if length and record.lengths:
        for tag, value in record.lengths.items():
            base[f"{tag}_len_m"] = value

    return base
