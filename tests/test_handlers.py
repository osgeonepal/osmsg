"""Process synthetic .osc files end-to-end through the handler and verify counts."""

from __future__ import annotations

import datetime as dt

import pytest

from osmsg.handlers import ChangefileHandler


def test_changefile_handler_counts_node_create_and_poi(osc_factory, changefile_config):
    """A v1 node with a tag → poi_created=1, nodes.c=1."""
    osc = osc_factory(
        "001.osc",
        [
            (
                "node",
                {"id": 100, "version": 1, "uid": 10, "user": "alice", "changeset": 1, "tags": {"amenity": "cafe"}},
            ),
            (
                "node",
                {"id": 101, "version": 1, "uid": 10, "user": "alice", "changeset": 1, "tags": {"amenity": "bench"}},
            ),
            # untagged → counts as node create but NOT poi
            ("node", {"id": 102, "version": 1, "uid": 10, "user": "alice", "changeset": 1, "tags": {}}),
        ],
    )

    handler = ChangefileHandler(changefile_config, sequence_id=1)
    handler.apply_file(str(osc))

    assert 1 in handler.stats
    s = handler.stats[1]
    assert s.nodes.c == 3
    assert s.nodes.m == 0
    assert s.nodes.d == 0
    assert s.poi_created == 2  # only the tagged ones
    assert s.poi_modified == 0


def test_changefile_handler_counts_way_modify_and_delete(osc_factory, changefile_config):
    osc = osc_factory(
        "002.osc",
        [
            # version=2 → modify
            (
                "way",
                {
                    "id": 1,
                    "version": 2,
                    "uid": 10,
                    "user": "alice",
                    "changeset": 5,
                    "nodes": [1, 2, 3],
                    "tags": {"highway": "residential"},
                },
            ),
            # version=0 means osmium signals deleted → DELETE
        ],
    )
    handler = ChangefileHandler(changefile_config, sequence_id=1)
    handler.apply_file(str(osc))

    s = handler.stats[5]
    assert s.ways.m == 1
    assert s.ways.c == 0


def test_changefile_handler_drops_data_outside_window(osc_factory, changefile_config):
    """Elements with timestamps outside [start_date_utc, end_date_utc) are ignored."""
    changefile_config["start_date_utc"] = dt.datetime(2026, 4, 1, tzinfo=dt.UTC)
    changefile_config["end_date_utc"] = dt.datetime(2026, 4, 2, tzinfo=dt.UTC)

    # Without an explicit timestamp, osmium's apply_file uses 1970 — outside our window.
    osc = osc_factory(
        "003.osc",
        [("node", {"id": 1, "version": 1, "uid": 10, "user": "alice", "changeset": 1, "tags": {"amenity": "cafe"}})],
    )
    handler = ChangefileHandler(changefile_config, sequence_id=1)
    handler.apply_file(str(osc))
    assert handler.stats == {}


def test_changefile_handler_tracks_specified_tag_keys(osc_factory, changefile_config):
    """--tags building highway → only those keys end up in tag_stats."""
    osc = osc_factory(
        "004.osc",
        [
            (
                "way",
                {
                    "id": 1,
                    "version": 1,
                    "uid": 10,
                    "user": "alice",
                    "changeset": 1,
                    "nodes": [1, 2],
                    "tags": {"building": "yes"},
                },
            ),
            (
                "way",
                {
                    "id": 2,
                    "version": 1,
                    "uid": 10,
                    "user": "alice",
                    "changeset": 1,
                    "nodes": [3, 4],
                    "tags": {"highway": "footway"},
                },
            ),
            # not in --tags list, must NOT appear in tag_stats
            (
                "way",
                {
                    "id": 3,
                    "version": 1,
                    "uid": 10,
                    "user": "alice",
                    "changeset": 1,
                    "nodes": [5, 6],
                    "tags": {"natural": "tree"},
                },
            ),
        ],
    )
    handler = ChangefileHandler(changefile_config, sequence_id=1)
    handler.apply_file(str(osc))

    s = handler.stats[1]
    assert "building" in s.tag_stats
    assert "highway" in s.tag_stats
    assert "natural" not in s.tag_stats
    assert s.tag_stats["building"]["yes"].c == 1
    assert s.tag_stats["highway"]["footway"].c == 1


def test_changefile_handler_all_tags_captures_everything(osc_factory, changefile_config):
    changefile_config["all_tags"] = True
    changefile_config["additional_tags"] = None
    osc = osc_factory(
        "005.osc",
        [
            (
                "node",
                {
                    "id": 1,
                    "version": 1,
                    "uid": 10,
                    "user": "alice",
                    "changeset": 1,
                    "tags": {"amenity": "cafe", "name": "X"},
                },
            ),
        ],
    )
    handler = ChangefileHandler(changefile_config, sequence_id=1)
    handler.apply_file(str(osc))
    assert set(handler.stats[1].tag_stats.keys()) == {"amenity", "name"}


def test_changefile_handler_whitelisted_users_filter(osc_factory, changefile_config):
    """--users alice → only her edits are recorded."""
    changefile_config["whitelisted_users"] = ["alice"]
    osc = osc_factory(
        "006.osc",
        [
            ("node", {"id": 1, "version": 1, "uid": 10, "user": "alice", "changeset": 1, "tags": {"amenity": "cafe"}}),
            ("node", {"id": 2, "version": 1, "uid": 20, "user": "bob", "changeset": 2, "tags": {"amenity": "bar"}}),
        ],
    )
    handler = ChangefileHandler(changefile_config, sequence_id=1)
    handler.apply_file(str(osc))
    assert set(handler.stats.keys()) == {1}
    assert set(u.username for u in handler.users.values()) == {"alice"}


def test_changefile_handler_valid_changesets_filter(osc_factory, changefile_config):
    """--hashtags pre-filter populates valid_changesets; only those changesets are kept."""
    osc = osc_factory(
        "007.osc",
        [
            ("node", {"id": 1, "version": 1, "uid": 10, "user": "alice", "changeset": 1, "tags": {"amenity": "cafe"}}),
            ("node", {"id": 2, "version": 1, "uid": 20, "user": "bob", "changeset": 2, "tags": {"amenity": "bar"}}),
        ],
    )
    handler = ChangefileHandler(changefile_config, sequence_id=1, valid_changesets={1})
    handler.apply_file(str(osc))
    assert set(handler.stats.keys()) == {1}


@pytest.mark.parametrize("version,expected_bucket", [(1, "c"), (2, "m"), (0, "d")])
def test_changefile_handler_action_dispatch(osc_factory, changefile_config, version, expected_bucket):
    """version=1 → CREATE, >1 → MODIFY, 0 → DELETE."""
    osc = osc_factory(
        f"action_{version}.osc",
        [
            (
                "node",
                {"id": 1, "version": version, "uid": 10, "user": "alice", "changeset": 1, "tags": {"amenity": "cafe"}},
            ),
        ],
    )
    handler = ChangefileHandler(changefile_config, sequence_id=1)
    handler.apply_file(str(osc))
    bucket = handler.stats[1].nodes
    assert getattr(bucket, expected_bucket) == 1
