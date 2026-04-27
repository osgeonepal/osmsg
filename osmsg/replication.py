"""OSM replication URL helpers — planet changefiles + planet changesets."""

from __future__ import annotations

import sys
from datetime import UTC, datetime

from osmium.replication.server import ReplicationServer

from ._http import session

PLANET_BASE = "https://planet.openstreetmap.org/replication"
SHORTCUTS = {
    "minute": f"{PLANET_BASE}/minute",
    "hour": f"{PLANET_BASE}/hour",
    "day": f"{PLANET_BASE}/day",
}
CHANGESETS_REPLICATION = f"{PLANET_BASE}/changesets/"


def resolve_url(value: str) -> str:
    """`minute|hour|day` → planet URL, else passthrough (after stripping trailing /)."""
    if value in SHORTCUTS:
        return SHORTCUTS[value]
    return value.rstrip("/")


def seq_to_timestamp(state_url: str) -> datetime:
    """Parse a replication state file and return its timestamp (UTC)."""
    txt = session.get(state_url).text
    start = txt.find("timestamp=") + len("timestamp=")
    end = txt.find("\n", start)
    raw = txt[start:end].replace("\\", "")
    return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def changefile_download_urls(
    start_date: datetime, end_date: datetime, base_url: str
) -> tuple[list[str], datetime, int, int, str, str]:
    """Return (urls, server_ts, start_seq, end_seq, start_seq_url, end_seq_url).

    For Geofabrik base URLs, public list-URLs are rewritten to the internal server
    (which carries uid/changeset_id metadata; the OAuth 2.0 cookie is required at fetch time).
    """
    repl = ReplicationServer(base_url)

    seq = repl.timestamp_to_sequence(start_date)
    if seq is None:
        sys.exit(f"Cannot reach replication service '{base_url}'")

    start_seq_time = seq_to_timestamp(repl.get_state_url(seq))
    if start_date > start_seq_time:
        if "minute" in base_url:
            seq = (seq + int((start_date - start_seq_time).total_seconds() / 60)) - 60
        elif "hour" in base_url:
            seq = (seq + int((start_date - start_seq_time).total_seconds() / 3600)) - 1

    start_seq = seq
    start_seq_url = repl.get_state_url(start_seq)

    state = repl.get_state_info()
    if state is None:
        sys.exit(f"Could not fetch state info from {base_url}")
    server_seq, server_ts = state
    server_ts = server_ts.astimezone(UTC)

    last_seq = server_seq
    if end_date:
        end_seq = repl.timestamp_to_sequence(end_date)
        if end_seq is None:
            sys.exit(f"Could not resolve end_date {end_date}")
        last_seq = end_seq
        if "minute" in base_url:
            adjust = int((seq_to_timestamp(repl.get_state_url(end_seq)) - end_date).total_seconds() / 60)
            last_seq = last_seq + adjust + 60
        else:
            last_seq += 1
        if last_seq >= server_seq:
            last_seq = server_seq

    if seq >= last_seq:
        return [], server_ts, start_seq, last_seq, start_seq_url, repl.get_state_url(last_seq)

    end_seq_url = repl.get_state_url(last_seq)
    urls = []
    is_geofabrik = "geofabrik" in base_url
    while seq <= last_seq:
        diff_url = repl.get_diff_url(seq)
        if is_geofabrik:
            diff_url = diff_url.replace("download.geofabrik", "osm-internal.download.geofabrik")
        urls.append(diff_url)
        seq += 1
    return urls, server_ts, start_seq, last_seq, start_seq_url, end_seq_url


class ChangesetReplication:
    """Planet changeset replication URL helper."""

    def __init__(self, base_url: str = CHANGESETS_REPLICATION) -> None:
        self.base = base_url

    def _state(self) -> tuple[int, datetime]:
        txt = session.get(self.base + "state.yaml").text
        seq = int(txt.split("sequence: ")[1])
        last_run = datetime.strptime(txt.split("last_run: ")[1][:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        return seq, last_run

    def _padded(self, seq: int) -> str:
        s = str(seq).zfill(9)
        return f"{s[:3]}/{s[3:6]}/{s[6:]}"

    def diff_url(self, seq: int) -> str:
        return f"{self.base}{self._padded(seq)}.osm.gz"

    def state_url(self, seq: int) -> str:
        return f"{self.base}{self._padded(seq)}.state.txt"

    def timestamp_to_sequence(self, ts: datetime) -> int:
        cur_seq, last_run = self._state()
        wanted = int((ts - last_run).total_seconds() / 60) + cur_seq
        return min(wanted, cur_seq)

    def sequence_to_timestamp(self, seq: int) -> datetime:
        txt = session.get(self.state_url(seq)).text
        return datetime.strptime(txt.split("last_run: ")[1][:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)

    def download_urls(self, start_date: datetime, end_date: datetime | None = None) -> tuple[list[str], int, int]:
        start_seq = self.timestamp_to_sequence(start_date)
        start_ts = self.sequence_to_timestamp(start_seq)
        if start_ts > start_date:
            start_seq -= int((start_ts - start_date).total_seconds() / 60)
            start_ts = self.sequence_to_timestamp(start_seq)
        if start_date > start_ts and (start_date - start_ts).seconds != 15 * 60:
            start_seq = start_seq + int((start_date - start_ts).total_seconds() / 60) - 60

        cur_seq, last_run = self._state()
        if end_date is None or end_date > last_run:
            end_seq = cur_seq
        else:
            end_seq = self.timestamp_to_sequence(end_date)
            end_ts = self.sequence_to_timestamp(end_seq)
            if end_date > end_ts:
                end_seq += int((end_date - end_ts).total_seconds() / 60) + 1
                end_ts = self.sequence_to_timestamp(end_seq)
            if end_ts > end_date:
                end_seq += int((end_ts - end_date).total_seconds() / 60) + 60
            end_seq = min(end_seq, cur_seq)

        if start_seq >= end_seq:
            return [], start_seq, end_seq

        urls = [self.diff_url(s) for s in range(start_seq, end_seq + 1)]
        return urls, start_seq, end_seq
