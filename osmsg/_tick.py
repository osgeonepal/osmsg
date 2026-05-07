"""Worker tick: bootstrap on first run, --update thereafter."""

import fcntl
import os
import shlex
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
    create_tables(conn)
    result = get_state(conn, source_url) is not None
    conn.close()
    return result


def _parse_arg(args: list[str], flag: str) -> str | None:
    for i, arg in enumerate(args):
        if arg == flag and i + 1 < len(args):
            return args[i + 1]
    return None


def main() -> int:
    extra_args = shlex.split(os.environ.get("OSMSG_EXTRA_ARGS", ""))
    bootstrap = os.environ.get("OSMSG_BOOTSTRAP", "hour")
    bootstrap_days = os.environ.get("OSMSG_BOOTSTRAP_DAYS")
    name = _parse_arg(extra_args, "--name") or "stats"
    out = Path(_parse_arg(extra_args, "--output-dir") or "/var/lib/osmsg")
    country = _parse_arg(extra_args, "--country")
    url = _parse_arg(extra_args, "--url") or "minute"

    out.mkdir(parents=True, exist_ok=True)

    lock_path = out / f"{name}.lock"
    lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("[osmsg-tick] previous tick still running, skipping", flush=True)
        return 0

    source_url = country_update_url(country) if country else resolve_url(url)
    db_path = out / f"{name}.duckdb"

    extra_set = set(extra_args)
    cmd = ["osmsg"] + extra_args
    if not (extra_set & {"--all", "--keys"}):
        cmd.append("--all")

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
