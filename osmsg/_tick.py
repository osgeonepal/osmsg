"""Worker tick: bootstrap on first run, --update thereafter."""

from __future__ import annotations

import fcntl
import os
import subprocess
import sys
from pathlib import Path

from .db import connect, create_tables, get_state
from .geofabrik import country_update_url
from .replication import resolve_url


def _has_state(db_path: Path, source_url: str) -> bool:
    if not db_path.exists():
        return False
    conn = connect(str(db_path))
    try:
        create_tables(conn)
        return get_state(conn, source_url) is not None
    finally:
        conn.close()


def main() -> int:
    name = os.environ.get("OSMSG_NAME", "stats")
    out = Path(os.environ.get("OSMSG_OUTPUT_DIR", "/var/lib/osmsg"))
    cache = Path(os.environ.get("OSMSG_CACHE_DIR", "/var/cache/osmsg"))
    country = os.environ.get("OSMSG_COUNTRY")
    url = os.environ.get("OSMSG_URL", "minute")
    boundary = os.environ.get("OSMSG_BOUNDARY")
    bootstrap = os.environ.get("OSMSG_BOOTSTRAP", "hour")
    bootstrap_days = os.environ.get("OSMSG_BOOTSTRAP_DAYS")
    psql_dsn = os.environ.get("DATABASE_URL")

    out.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)

    lock_path = out / f"{name}.lock"
    lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("[osmsg-tick] previous tick still running, skipping", flush=True)
        return 0

    db_path = out / f"{name}.duckdb"
    cmd = [
        "osmsg",
        "--name",
        name,
        "--output-dir",
        str(out),
        "--cache-dir",
        str(cache),
        "--format",
        "parquet",
    ]
    if country:
        cmd.extend(["--country", country])
        source_url = country_update_url(country)
    else:
        cmd.extend(["--url", url])
        source_url = resolve_url(url)
    if boundary:
        cmd.extend(["--boundary", boundary])
    if psql_dsn:
        cmd.extend(["--format", "psql", "--psql-dsn", psql_dsn])

    if _has_state(db_path, source_url):
        cmd.append("--update")
    elif bootstrap_days:
        cmd.extend(["--days", bootstrap_days])
    else:
        cmd.extend(["--last", bootstrap])

    print(f"[osmsg-tick] {' '.join(cmd)}", flush=True)
    return subprocess.call(cmd)


if __name__ == "__main__":
    sys.exit(main())
