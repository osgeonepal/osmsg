import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()


PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    uid      BIGINT PRIMARY KEY,
    username TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS changesets (
    changeset_id BIGINT PRIMARY KEY,
    uid          BIGINT NOT NULL REFERENCES users(uid),
    created_at   TIMESTAMPTZ,
    hashtags     TEXT[],
    editor       TEXT,
    min_lon      DOUBLE PRECISION,
    min_lat      DOUBLE PRECISION,
    max_lon      DOUBLE PRECISION,
    max_lat      DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_changesets_created_at ON changesets(created_at);
CREATE TABLE IF NOT EXISTS changeset_stats (
    changeset_id   BIGINT NOT NULL REFERENCES changesets(changeset_id),
    seq_id         BIGINT NOT NULL,
    uid            BIGINT NOT NULL REFERENCES users(uid),
    nodes_created  INTEGER DEFAULT 0,
    nodes_modified INTEGER DEFAULT 0,
    nodes_deleted  INTEGER DEFAULT 0,
    ways_created   INTEGER DEFAULT 0,
    ways_modified  INTEGER DEFAULT 0,
    ways_deleted   INTEGER DEFAULT 0,
    rels_created   INTEGER DEFAULT 0,
    rels_modified  INTEGER DEFAULT 0,
    rels_deleted   INTEGER DEFAULT 0,
    poi_created    INTEGER DEFAULT 0,
    poi_modified   INTEGER DEFAULT 0,
    tag_stats      JSONB,
    PRIMARY KEY (seq_id, changeset_id)
);
CREATE INDEX IF NOT EXISTS idx_changeset_stats_uid ON changeset_stats(uid);
CREATE TABLE IF NOT EXISTS state (
    source_url  TEXT PRIMARY KEY,
    last_seq    BIGINT NOT NULL,
    last_ts     TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);
"""

_pool: asyncpg.Pool | None = None


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    return database_url


async def open_pool() -> None:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=get_database_url(), min_size=1, max_size=10)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool is not initialized")
    return _pool


async def ensure_schema() -> None:
    statements = [s.strip() for s in PG_SCHEMA.strip().split(";") if s.strip()]
    async with get_pool().acquire() as conn:
        for stmt in statements:
            await conn.execute(stmt)
