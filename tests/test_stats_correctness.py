"""Stats correctness — the central guarantees osmsg has to hold:

1. Hand-counted fixtures match the queried output exactly.
2. Processing the same .osc file twice does not double-count or drop anything.
3. Multi-worker (parallel) processing produces identical totals to single-worker.
4. Long-running changesets that span multiple replication files aggregate correctly.
5. No user is ever silently dropped — every uid in the input shows up in user_stats.
"""

from __future__ import annotations

import duckdb

from osmsg.db.ingest import flush_rows_to_parquet, merge_parquet_files
from osmsg.db.queries import user_stats
from osmsg.db.schema import create_tables
from osmsg.handlers import ChangefileHandler


def _flush(handler: ChangefileHandler, parquet_dir, pid: int = 1, batch: int = 1):
    flush_rows_to_parquet(
        parquet_dir=parquet_dir,
        pid=pid,
        batch_index=batch,
        users=[u.to_row() for u in handler.users.values()],
        changesets=[c.to_row() for c in handler.stubs.values()],
        changeset_stats=[s.to_row() for s in handler.stats.values()],
    )


# 1) Hand-counted exact match


def test_user_stats_match_hand_counted_changes(tmp_path, osc_factory, changefile_config):
    """Build a deterministic .osc, run the full pipeline, assert every counter."""
    changefile_config["all_tags"] = True
    changefile_config["additional_tags"] = None

    osc = osc_factory(
        "hand.osc",
        [
            # 5 node creates, 3 tagged → poi_created=3
            (
                "node",
                {"id": 1, "version": 1, "uid": 100, "user": "alice", "changeset": 1000, "tags": {"amenity": "cafe"}},
            ),
            (
                "node",
                {"id": 2, "version": 1, "uid": 100, "user": "alice", "changeset": 1000, "tags": {"shop": "bakery"}},
            ),
            (
                "node",
                {"id": 3, "version": 1, "uid": 100, "user": "alice", "changeset": 1000, "tags": {"amenity": "bench"}},
            ),
            ("node", {"id": 4, "version": 1, "uid": 100, "user": "alice", "changeset": 1000, "tags": {}}),
            ("node", {"id": 5, "version": 1, "uid": 100, "user": "alice", "changeset": 1000, "tags": {}}),
            # 2 node modifies, 1 tagged → poi_modified=1
            (
                "node",
                {"id": 6, "version": 2, "uid": 100, "user": "alice", "changeset": 1000, "tags": {"natural": "tree"}},
            ),
            ("node", {"id": 7, "version": 2, "uid": 100, "user": "alice", "changeset": 1000, "tags": {}}),
            # 1 node delete
            ("node", {"id": 8, "version": 0, "uid": 100, "user": "alice", "changeset": 1000, "tags": {}}),
            # 3 way creates
            (
                "way",
                {
                    "id": 10,
                    "version": 1,
                    "uid": 100,
                    "user": "alice",
                    "changeset": 1000,
                    "nodes": [1, 2, 3],
                    "tags": {"highway": "footway"},
                },
            ),
            (
                "way",
                {
                    "id": 11,
                    "version": 1,
                    "uid": 100,
                    "user": "alice",
                    "changeset": 1000,
                    "nodes": [4, 5, 6],
                    "tags": {"building": "yes"},
                },
            ),
            (
                "way",
                {
                    "id": 12,
                    "version": 1,
                    "uid": 100,
                    "user": "alice",
                    "changeset": 1000,
                    "nodes": [7, 8, 9],
                    "tags": {},
                },
            ),
            # 1 way modify
            (
                "way",
                {"id": 13, "version": 2, "uid": 100, "user": "alice", "changeset": 1000, "nodes": [1, 2], "tags": {}},
            ),
            # 1 relation create
            (
                "relation",
                {"id": 20, "version": 1, "uid": 100, "user": "alice", "changeset": 1000, "members": [], "tags": {}},
            ),
        ],
    )

    handler = ChangefileHandler(changefile_config, sequence_id=999)
    handler.apply_file(str(osc))

    # In-memory invariants
    s = handler.stats[1000]
    assert (s.nodes.c, s.nodes.m, s.nodes.d) == (5, 2, 1)
    assert (s.ways.c, s.ways.m, s.ways.d) == (3, 1, 0)
    assert (s.rels.c, s.rels.m, s.rels.d) == (1, 0, 0)
    assert s.poi_created == 3
    assert s.poi_modified == 1

    # Query-layer invariants (full round-trip through Parquet → DuckDB → SQL)
    parquet_dir = tmp_path / "parq"
    _flush(handler, parquet_dir)
    db = duckdb.connect(str(tmp_path / "test.duckdb"))
    create_tables(db)
    merge_parquet_files(db, parquet_dir, cleanup=False)

    rows = user_stats(db)
    assert len(rows) == 1
    r = rows[0]
    assert r["name"] == "alice"
    assert r["changesets"] == 1
    assert r["nodes_create"] == 5
    assert r["nodes_modify"] == 2
    assert r["nodes_delete"] == 1
    assert r["ways_create"] == 3
    assert r["ways_modify"] == 1
    assert r["ways_delete"] == 0
    assert r["rels_create"] == 1
    assert r["poi_create"] == 3
    assert r["poi_modify"] == 1
    # map_changes is the sum of the nine element columns — never the POI ones.
    assert r["map_changes"] == 5 + 2 + 1 + 3 + 1 + 0 + 1 + 0 + 0


# 2) Idempotency: same file processed twice


def test_processing_same_file_twice_yields_identical_stats(tmp_path, osc_factory, changefile_config):
    osc = osc_factory(
        "idem.osc",
        [
            ("node", {"id": 1, "version": 1, "uid": 10, "user": "alice", "changeset": 1, "tags": {"amenity": "cafe"}}),
            ("node", {"id": 2, "version": 1, "uid": 10, "user": "alice", "changeset": 1, "tags": {}}),
            (
                "way",
                {
                    "id": 1,
                    "version": 1,
                    "uid": 10,
                    "user": "alice",
                    "changeset": 1,
                    "nodes": [1, 2],
                    "tags": {"highway": "track"},
                },
            ),
        ],
    )

    db = duckdb.connect(str(tmp_path / "test.duckdb"))
    create_tables(db)

    for run_id in range(2):
        handler = ChangefileHandler(changefile_config, sequence_id=42)  # SAME seq_id both times
        handler.apply_file(str(osc))
        _flush(handler, tmp_path / f"parq_{run_id}")
        merge_parquet_files(db, tmp_path / f"parq_{run_id}", cleanup=False)

    rows = user_stats(db)
    assert len(rows) == 1
    alice = rows[0]
    assert alice["nodes_create"] == 2
    assert alice["ways_create"] == 1
    assert alice["map_changes"] == 3  # 2 + 1
    assert alice["changesets"] == 1

    # Defensive: the underlying changeset_stats table must contain exactly one row.
    cs_count = db.execute("SELECT COUNT(*) FROM changeset_stats").fetchone()[0]
    assert cs_count == 1, "INSERT OR IGNORE failed — duplicate stats rows from second pass"


# 3) Parallel equivalence: multiple workers vs single worker


def test_multi_worker_pipeline_matches_single_worker(tmp_path, osc_factory, changefile_config):
    """Same input across 4 files; processing them with 4 different pids must equal serial output."""
    files = []
    for f_idx in range(4):
        items = []
        for i in range(3):
            items.append(
                (
                    "node",
                    {
                        "id": (f_idx * 100) + i + 1,
                        "version": 1,
                        "uid": 10 + (i % 2),  # 2 distinct uids
                        "user": f"user{i % 2}",
                        "changeset": (f_idx * 10) + i + 1,
                        "tags": {"amenity": "cafe"} if i == 0 else {},
                    },
                )
            )
        files.append(osc_factory(f"f{f_idx}.osc", items))

    def run(parquet_dir, pid_per_file: bool):
        db = duckdb.connect(":memory:")
        create_tables(db)
        for i, f in enumerate(files):
            handler = ChangefileHandler(changefile_config, sequence_id=i + 1)
            handler.apply_file(str(f))
            _flush(handler, parquet_dir, pid=(10 + i) if pid_per_file else 1, batch=i + 1)
        merge_parquet_files(db, parquet_dir, cleanup=False)
        return sorted(user_stats(db), key=lambda r: r["uid"])

    serial = run(tmp_path / "parq_serial", pid_per_file=False)
    parallel = run(tmp_path / "parq_parallel", pid_per_file=True)

    assert len(serial) == len(parallel) == 2
    for sr, pr in zip(serial, parallel, strict=True):
        for k in ("uid", "name", "changesets", "nodes_create", "ways_create", "map_changes", "poi_create"):
            assert sr[k] == pr[k], f"mismatch on {k}: serial={sr[k]} parallel={pr[k]}"


# 4) Multi-file aggregation per user


def test_changeset_spread_across_files_aggregates(tmp_path, osc_factory, changefile_config):
    """Two replication files, same user, two distinct changesets → counts must sum."""
    f1 = osc_factory(
        "f1.osc",
        [
            ("node", {"id": 1, "version": 1, "uid": 10, "user": "alice", "changeset": 1, "tags": {"amenity": "cafe"}}),
        ],
    )
    f2 = osc_factory(
        "f2.osc",
        [
            ("node", {"id": 2, "version": 1, "uid": 10, "user": "alice", "changeset": 2, "tags": {}}),
            ("way", {"id": 10, "version": 1, "uid": 10, "user": "alice", "changeset": 2, "nodes": [1, 2], "tags": {}}),
        ],
    )

    db = duckdb.connect(str(tmp_path / "agg.duckdb"))
    create_tables(db)
    for i, (f, seq) in enumerate([(f1, 100), (f2, 200)]):
        handler = ChangefileHandler(changefile_config, sequence_id=seq)
        handler.apply_file(str(f))
        _flush(handler, tmp_path / f"parq_{i}", pid=i)
        merge_parquet_files(db, tmp_path / f"parq_{i}", cleanup=False)

    rows = user_stats(db)
    assert len(rows) == 1
    alice = rows[0]
    assert alice["changesets"] == 2  # COUNT(DISTINCT changeset_id)
    assert alice["nodes_create"] == 2  # 1 from each file
    assert alice["ways_create"] == 1  # only in f2
    assert alice["poi_create"] == 1  # only the tagged node
    assert alice["map_changes"] == 3


# 5) No user is silently dropped


def test_every_uid_in_input_appears_in_user_stats(tmp_path, osc_factory, changefile_config):
    items = []
    for i in range(10):
        items.append(
            (
                "node",
                {
                    "id": i + 1,
                    "version": 1,
                    "uid": 100 + i,
                    "user": f"user{i}",
                    "changeset": i + 1,
                    "tags": {"amenity": "cafe"},
                },
            )
        )
    osc = osc_factory("ten.osc", items)

    handler = ChangefileHandler(changefile_config, sequence_id=999)
    handler.apply_file(str(osc))
    _flush(handler, tmp_path / "parq")

    db = duckdb.connect(str(tmp_path / "ten.duckdb"))
    create_tables(db)
    merge_parquet_files(db, tmp_path / "parq", cleanup=False)

    rows = user_stats(db)
    assert len(rows) == 10
    assert {r["name"] for r in rows} == {f"user{i}" for i in range(10)}
    assert {r["uid"] for r in rows} == {100 + i for i in range(10)}
    # Every user should have exactly 1 changeset, 1 poi_create
    assert all(r["changesets"] == 1 and r["poi_create"] == 1 for r in rows)


# 6) Different-pid worker shards are all picked up by merge


def test_merge_picks_up_every_worker_shard(tmp_path, osc_factory, changefile_config):
    """Simulates 8 worker processes writing to the same parquet dir; nothing must be missed."""
    osc = osc_factory(
        "shared.osc",
        [
            ("node", {"id": 1, "version": 1, "uid": 10, "user": "alice", "changeset": 1, "tags": {}}),
        ],
    )

    parquet_dir = tmp_path / "shards"
    for pid in range(8):
        # Each worker processes the same file but emits a different (seq_id, changeset_id) row
        handler = ChangefileHandler(changefile_config, sequence_id=1000 + pid)
        handler.apply_file(str(osc))
        _flush(handler, parquet_dir, pid=pid, batch=1)

    db = duckdb.connect(":memory:")
    create_tables(db)
    merge_parquet_files(db, parquet_dir, cleanup=False)

    cs_count = db.execute("SELECT COUNT(*) FROM changeset_stats").fetchone()[0]
    assert cs_count == 8, f"expected 8 (seq_id, changeset_id) rows from 8 shards, got {cs_count}"

    rows = user_stats(db)
    assert len(rows) == 1
    assert rows[0]["changesets"] == 1  # COUNT(DISTINCT changeset_id), not COUNT(*)
    # nodes_create sums across all 8 shards
    assert rows[0]["nodes_create"] == 8
