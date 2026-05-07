import os

import asyncpg
from dotenv import load_dotenv

from .pg_schema import PG_SCHEMA

load_dotenv()

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
