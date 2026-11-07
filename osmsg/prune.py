"""Prune Postgres of rows now served from published history, keeping it to the uncovered live tail."""

import datetime as dt
import math

from .db.pg import connect_postgres, pg_execute
from .exceptions import OsmsgError
from .history import fetch_manifest
from .ui import info

DEFAULT_OVERLAP = dt.timedelta(days=2)
PRUNE_BATCH_SIZE = 50_000


def prune_pg(dsn: str, cutoff: dt.datetime, *, batch_size: int = PRUNE_BATCH_SIZE) -> tuple[int, int]:
    """Delete changesets older than cutoff with their changeset_stats, batch_size per committed statement.
    The DSN is interpolated into ATTACH, so it must be trusted."""
    iso = cutoff.astimezone(dt.UTC).isoformat()
    older = f"created_at < TIMESTAMPTZ '{iso}'"

    reader = connect_postgres(dsn)
    stats_row = reader.execute(
        "SELECT count(*) FROM pg.changeset_stats s "
        f"WHERE EXISTS (SELECT 1 FROM pg.changesets c WHERE c.changeset_id = s.changeset_id AND c.{older})"
    ).fetchone()
    cs_row = reader.execute(f"SELECT count(*) FROM pg.changesets WHERE {older}").fetchone()
    reader.close()
    stats_n = stats_row[0] if stats_row else 0
    cs_n = cs_row[0] if cs_row else 0

    if cs_n:
        writer = connect_postgres(dsn)
        # Each batch commits alone, so a statement timeout loses one batch, not the whole prune.
        for _ in range(math.ceil(cs_n / batch_size)):
            pg_execute(
                writer,
                f"WITH batch AS (SELECT changeset_id FROM changesets WHERE {older} LIMIT {int(batch_size)}), "
                "stats AS (DELETE FROM changeset_stats s USING batch b WHERE s.changeset_id = b.changeset_id) "
                "DELETE FROM changesets c USING batch b WHERE c.changeset_id = b.changeset_id",
            )
        writer.close()
    return stats_n, cs_n


def prune_covered(dsn: str, history_url: str, overlap: dt.timedelta = DEFAULT_OVERLAP) -> tuple[int, int]:
    """Prune Postgres up to the published frontier minus the overlap buffer."""
    manifest = fetch_manifest(history_url)
    if manifest is None:
        raise OsmsgError(f"could not read the manifest at {history_url}")
    cutoff = manifest.frontier - overlap
    stats_n, cs_n = prune_pg(dsn, cutoff)
    info(f"prune: cutoff {cutoff.astimezone(dt.UTC).isoformat()} -> deleted {cs_n:,} changesets, {stats_n:,} stats")
    return stats_n, cs_n
