"""Postgres pruning derives the cutoff from the published frontier minus the overlap buffer."""

import datetime as dt
import os

import pytest

import osmsg.prune as prune
from osmsg.db.pg import connect_postgres, pg_execute
from osmsg.exceptions import OsmsgError
from osmsg.history import Manifest
from osmsg.pg_schema import PG_SCHEMA, PG_TAG_TYPE_SQL

UTC = dt.UTC


def test_prune_covered_computes_cutoff(monkeypatch):
    frontier = dt.datetime(2026, 8, 1, tzinfo=UTC)
    monkeypatch.setattr(prune, "fetch_manifest", lambda url: Manifest(dt.datetime(2005, 4, 1, tzinfo=UTC), frontier))
    captured = {}

    def fake_prune_pg(dsn, cutoff):
        captured["dsn"], captured["cutoff"] = dsn, cutoff
        return (7, 4)

    monkeypatch.setattr(prune, "prune_pg", fake_prune_pg)
    stats_n, cs_n = prune.prune_covered("mydsn", "hf://x", overlap=dt.timedelta(days=2))

    assert captured["cutoff"] == frontier - dt.timedelta(days=2)  # keep 2 days beyond the frontier
    assert captured["dsn"] == "mydsn"
    assert (stats_n, cs_n) == (7, 4)


def test_prune_covered_raises_without_manifest(monkeypatch):
    monkeypatch.setattr(prune, "fetch_manifest", lambda url: None)
    with pytest.raises(OsmsgError):
        prune.prune_covered("dsn", "url")


class _FakeConn:
    def __init__(self, calls):
        self._calls = calls

    def execute(self, sql):
        self._calls.append(sql)
        return self

    def fetchone(self):
        return (5,)

    def close(self):
        pass


def test_prune_pg_runs_one_native_delete_per_batch(monkeypatch):
    calls = []
    monkeypatch.setattr(prune, "connect_postgres", lambda *a, **k: _FakeConn(calls))
    cutoff = dt.datetime(2026, 7, 30, tzinfo=UTC)

    def delete_count(batch_size):
        calls.clear()
        assert prune.prune_pg("dsn", cutoff, batch_size=batch_size) == (5, 5)
        return sum("postgres_execute" in c and "DELETE" in c for c in calls)

    assert [delete_count(2), delete_count(5), delete_count(10)] == [3, 1, 1]


@pytest.mark.network
@pytest.mark.skipif(not os.environ.get("OSMSG_PG_DSN"), reason="OSMSG_PG_DSN not set; live PG prune not exercised")
def test_prune_pg_deletes_exactly_old_rows_across_batches():
    dsn = os.environ["OSMSG_PG_DSN"]
    con = connect_postgres(dsn)
    for table in ("changeset_hashtag", "changeset_stats", "changesets", "users", "state"):
        pg_execute(con, f"DROP TABLE IF EXISTS {table} CASCADE")
    pg_execute(con, "DROP TYPE IF EXISTS osmsg_tag CASCADE")
    pg_execute(con, PG_TAG_TYPE_SQL)
    pg_execute(con, PG_SCHEMA)
    pg_execute(con, "INSERT INTO users VALUES (1, 'u')")
    pg_execute(
        con,
        "INSERT INTO changesets (changeset_id, uid, created_at) "
        "SELECT i, 1, TIMESTAMPTZ '2026-01-01' + i * INTERVAL '1 day' FROM generate_series(1, 10) i",
    )
    pg_execute(
        con, "INSERT INTO changeset_stats (changeset_id, seq_id, uid) SELECT i, 0, 1 FROM generate_series(1, 10) i"
    )
    pg_execute(con, "INSERT INTO changeset_hashtag SELECT '#x', i, NULL FROM generate_series(1, 10) i")
    con.close()

    cutoff = dt.datetime(2026, 1, 9, tzinfo=UTC)
    assert prune.prune_pg(dsn, cutoff, batch_size=2) == (7, 7)

    check = connect_postgres(dsn, read_only=True)
    remaining = {
        table: sorted(r[0] for r in check.execute(f"SELECT changeset_id FROM pg.{table}").fetchall())
        for table in ("changesets", "changeset_stats", "changeset_hashtag")
    }
    check.close()
    assert remaining == {"changesets": [8, 9, 10], "changeset_stats": [8, 9, 10], "changeset_hashtag": [8, 9, 10]}
