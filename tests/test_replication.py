"""ChangesetReplication URL math — verifies the 24h backward pad invariant.

OSM caps changeset open time at 24 hours. A still-open changeset created near the
24h boundary before our window can still have its first edits land in the window;
without the 24h backward pad, its open=true metadata entry sits before our cached
range, valid_changesets misses it, and the changefile filter silently drops the
in-window edits.
"""

from __future__ import annotations

import datetime as dt

import pytest

from osmsg.replication import ChangesetReplication


@pytest.fixture
def repl(monkeypatch):
    """Stub the network: 1 sequence == 1 minute, anchored at a fixed cur_seq/last_run."""
    cur_seq = 1_000_000
    last_run = dt.datetime(2026, 4, 27, 22, 0, tzinfo=dt.UTC)
    r = ChangesetReplication()

    def fake_state():
        return cur_seq, last_run

    def fake_seq_to_ts(seq):
        return last_run + dt.timedelta(minutes=(seq - cur_seq))

    monkeypatch.setattr(r, "_state", fake_state)
    monkeypatch.setattr(r, "sequence_to_timestamp", fake_seq_to_ts)
    return r, cur_seq, last_run


def test_download_urls_pads_backward_24h(repl):
    """The first downloaded sequence must be ≥ 24h before start_date so any
    changeset created up to 24h before is reachable from cache."""
    r, cur_seq, last_run = repl
    start = dt.datetime(2026, 4, 27, 21, 4, tzinfo=dt.UTC)
    end = dt.datetime(2026, 4, 27, 21, 54, tzinfo=dt.UTC)

    urls, start_seq, end_seq = r.download_urls(start, end)

    start_seq_ts = last_run + dt.timedelta(minutes=(start_seq - cur_seq))
    backward = start - start_seq_ts
    assert backward >= dt.timedelta(hours=24), (
        f"backward pad must be ≥ 24h to catch long-running changesets, got {backward}"
    )
    # Forward end seq should land on or just past end_date.
    end_seq_ts = last_run + dt.timedelta(minutes=(end_seq - cur_seq))
    assert end_seq_ts >= end


def test_download_urls_caps_end_at_cur_seq(repl):
    """Future end_date can't fetch beyond the server's current sequence."""
    r, cur_seq, _ = repl
    start = dt.datetime(2026, 4, 27, 21, 0, tzinfo=dt.UTC)
    end = dt.datetime(2099, 1, 1, tzinfo=dt.UTC)

    _, _, end_seq = r.download_urls(start, end)
    assert end_seq <= cur_seq
