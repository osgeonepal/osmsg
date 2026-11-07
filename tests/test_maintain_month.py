"""maintain month completeness guard: never publish a month whose data stops short of its boundary."""

import datetime as dt

import duckdb
import pytest

from osmsg.db.schema import create_tables
from osmsg.exceptions import OsmsgError
from osmsg.maintain.month import verify_month_complete

UTC = dt.UTC


def _db_with_last_changeset(tmp_path, created_at):
    db = tmp_path / "m.duckdb"
    con = duckdb.connect(str(db))
    create_tables(con)
    con.execute(
        "INSERT INTO changesets (changeset_id, uid, created_at) VALUES (1, 1, ?)",
        [created_at],
    )
    con.close()
    return db


def test_complete_month_passes(tmp_path):
    db = _db_with_last_changeset(tmp_path, dt.datetime(2026, 5, 31, 23, 59, tzinfo=UTC))
    verify_month_complete(db, 2026, 5)


def test_truncated_month_raises(tmp_path):
    db = _db_with_last_changeset(tmp_path, dt.datetime(2026, 5, 31, 22, 0, tzinfo=UTC))
    with pytest.raises(OsmsgError, match="incomplete"):
        verify_month_complete(db, 2026, 5)


def test_empty_month_raises(tmp_path):
    db = tmp_path / "e.duckdb"
    con = duckdb.connect(str(db))
    create_tables(con)
    con.close()
    with pytest.raises(OsmsgError):
        verify_month_complete(db, 2026, 5)


def test_upload_sends_partitions_as_folders_and_rollups_as_files(tmp_path, monkeypatch):
    from osmsg.maintain import month

    for dataset in ("changefiles", "changesets"):
        (tmp_path / dataset / "year=2026" / "month=10").mkdir(parents=True)
    for rollup in ("rollup/hashtag_changeset/data.parquet", "rollup/users/data.parquet"):
        (tmp_path / rollup).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rollup).write_bytes(b"x")
    calls = []
    monkeypatch.setattr(month.huggingface_hub, "create_repo", lambda repo, **kw: calls.append(("repo", repo, kw)))
    monkeypatch.setattr(month.huggingface_hub, "upload_folder", lambda **kw: calls.append(("folder", kw)))
    monkeypatch.setattr(month.huggingface_hub, "upload_file", lambda **kw: calls.append(("file", kw)))

    month.upload("org/history", tmp_path, 2026, 10)

    assert calls[0] == ("repo", "org/history", {"repo_type": "dataset", "exist_ok": True})
    sent = {(kind, kw["path_in_repo"], kw["repo_id"], kw["repo_type"]) for kind, kw in calls[1:]}
    assert sent == {
        ("folder", "changefiles/year=2026/month=10", "org/history", "dataset"),
        ("folder", "changesets/year=2026/month=10", "org/history", "dataset"),
        ("file", "rollup/hashtag_changeset/data.parquet", "org/history", "dataset"),
        ("file", "rollup/users/data.parquet", "org/history", "dataset"),
    }
