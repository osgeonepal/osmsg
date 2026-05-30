# API query performance checks

Use this script to run `EXPLAIN ANALYZE` for the analytics queries against a real PostgreSQL database.

```powershell
$env:DATABASE_URL="postgresql://..."
uv run --group api python scripts/check_api_query_performance.py --days 30 --limit 100
```

If the database was created before the analytics indexes were added, include `--ensure-indexes` before measuring:

```powershell
uv run --group api python scripts/check_api_query_performance.py --days 30 --limit 100 --ensure-indexes
```

Check a specific hashtag:

```powershell
uv run --group api python scripts/check_api_query_performance.py --days 30 --hashtag maproulette --limit 100 --ensure-indexes
```

Check a fixed window:

```powershell
uv run --group api python scripts/check_api_query_performance.py --start 2026-05-01T00:00:00Z --end 2026-05-08T00:00:00Z --limit 100
```

Check with generated temporary data instead of the real tables:

```powershell
uv run --group api python scripts/check_api_query_performance.py --synthetic-rows 1000000 --start 2026-05-01T00:00:00Z --end 2026-05-31T00:00:00Z --limit 100
```

Synthetic mode creates temporary tables in the current PostgreSQL session, runs the same query plans, and drops the temporary data when the connection closes.

The output includes table counts and PostgreSQL execution plans for:

- hashtag stats
- hashtag trends
- editor stats

When reviewing the output, pay attention to:

- total execution time
- whether indexes are used for `changesets.created_at`, `changesets.hashtags`, and `changeset_stats.changeset_id`
- whether row counts become too large for broad date windows
- whether a smaller time window or cached aggregate table is needed for planet-scale data
