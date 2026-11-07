"""Monthly hashtag_changeset and users rollups the API reads for hashtag queries. See docs/rollups.md."""

import pathlib

import duckdb

from ..db.schema import _apply_runtime_pragmas
from ..exceptions import OsmsgError
from ..stats import COUNT_COLS as _COUNT_COLS
from .parquet import ROW_GROUP_SIZE


def _copy(con: duckdb.DuckDBPyConnection, path: pathlib.Path, select: str, order_by: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({select} ORDER BY {order_by}) TO '{path}' (FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP_SIZE})")


def build_month_rollups(year: int, month: int, out: pathlib.Path) -> None:
    """Build the hashtag_changeset and users rollups for one month from its raw partitions. Idempotent:
    a re-run replaces the month's rows."""
    changefiles = out / "changefiles" / f"year={year}" / f"month={month}" / "data.parquet"
    changesets = out / "changesets" / f"year={year}" / f"month={month}" / "data.parquet"
    if not changefiles.exists() or not changesets.exists():
        raise OsmsgError(f"missing raw partition for {year:04d}-{month:02d}; export the month first")

    con = duckdb.connect()
    _apply_runtime_pragmas(con)
    con.execute(f"CREATE VIEW cf AS SELECT * FROM read_parquet('{changefiles}')")
    con.execute(f"CREATE VIEW cs AS SELECT * FROM read_parquet('{changesets}')")

    _refresh_hashtag_changeset(con, out)
    _refresh_users(con, out)
    con.close()


def _refresh_hashtag_changeset(con: duckdb.DuckDBPyConnection, out: pathlib.Path) -> None:
    """Merge this month's (hashtag, changeset) rows into the single file, replacing the month on a re-run. One
    hashtag-sorted file so a prefix range prunes and a remote query opens one footer."""
    hc = out / "rollup" / "hashtag_changeset" / "data.parquet"
    cf_counts = ", ".join(f"cf.{c}" for c in _COUNT_COLS)
    con.execute(
        f"""CREATE TABLE month_hc AS
            SELECT lower(h) AS hashtag, cf.changeset_id, cf.uid, cs.editor, cf.created_at,
                   {cf_counts}, cf.tags, cs.lon, cs.lat
            FROM cf JOIN cs USING (changeset_id) CROSS JOIN UNNEST(cs.hashtags) AS t(h)
            WHERE cs.hashtags IS NOT NULL"""
    )
    if hc.exists():
        con.execute(f"CREATE TABLE prev_hc AS SELECT * FROM read_parquet('{hc}')")
        select = (
            "SELECT * FROM prev_hc WHERE changeset_id NOT IN (SELECT changeset_id FROM month_hc) "
            "UNION ALL SELECT * FROM month_hc"
        )
    else:
        select = "SELECT * FROM month_hc"
    _copy(con, hc, select, "hashtag")


def _refresh_users(con: duckdb.DuckDBPyConnection, out: pathlib.Path) -> None:
    """Map uid to username; the month just built wins for its users, others keep their prior name."""
    users = out / "rollup" / "users" / "data.parquet"
    con.execute(
        "CREATE TABLE month_users AS "
        "SELECT uid, any_value(username) AS username FROM cs WHERE username IS NOT NULL GROUP BY uid"
    )
    if users.exists():
        con.execute(f"CREATE TABLE prev_users AS SELECT * FROM read_parquet('{users}')")
        select = (
            "SELECT uid, username FROM prev_users WHERE uid NOT IN (SELECT uid FROM month_users) "
            "UNION ALL SELECT uid, username FROM month_users"
        )
    else:
        select = "SELECT uid, username FROM month_users"
    _copy(con, users, select, "uid")
