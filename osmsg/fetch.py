"""Download + decompress OSM replication files. Cache-friendly: skips downloads
when the decompressed file is already on disk."""

from __future__ import annotations

import gzip
import shutil
from pathlib import Path

from ._http import session

DEFAULT_CACHE_DIR = Path("temp")


def file_path_for(url: str, mode: str, cache_dir: Path = DEFAULT_CACHE_DIR) -> Path:
    parts = url.split("/")
    return cache_dir / mode / parts[-4] / f"{parts[-3]}_{parts[-2]}_{parts[-1]}"


def download_osm_file(
    url: str, mode: str = "changefiles", cookie: str | None = None, cache_dir: Path = DEFAULT_CACHE_DIR
) -> Path:
    """Fetch a `.osc.gz` / `.osm.gz`, decompress, return the decompressed path.

    If the decompressed file already exists, it's returned as-is (offline-friendly).
    """
    gz_path = file_path_for(url, mode, cache_dir)
    raw_path = gz_path.with_suffix("")

    if raw_path.exists():
        return raw_path

    if not gz_path.exists():
        headers = {"Cookie": cookie} if cookie and "geofabrik" in url.lower() else None
        r = session.get(url, headers=headers)
        r.raise_for_status()
        gz_path.parent.mkdir(parents=True, exist_ok=True)
        gz_path.write_bytes(r.content)

    with gzip.open(gz_path, "rb") as src, raw_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    gz_path.unlink(missing_ok=True)
    return raw_path
