# osmsg

OpenStreetMap stats generator. UTC-only, parquet-first, OAuth 2.0.

## Install

```bash
pip install osmsg
# or
uv tool install osmsg
```

## Quick start (CLI)

```bash
# Last hour, planet replication
osmsg --last hour

# Country-level (live Geofabrik index lookup, OAuth 2.0)
export OSM_USERNAME=... OSM_PASSWORD=...     # or use a .env
osmsg --country nepal --last day

# Custom range + per-key tag totals + daily summary
osmsg --start "2026-04-01 00:00:00" --end "2026-04-08 00:00:00" \
      --tags building --tags highway --summary

# Long flag lists are easier as YAML; CLI args still override
osmsg --config nepal.yaml --rows 50

# Cron-friendly: resume from where the last run left off
osmsg --country nepal --update
```

## Library usage

```python
from datetime import datetime, UTC

from osmsg import RunConfig, run, OsmsgError

cfg = RunConfig(
    name="nepal",
    countries=["nepal"],
    start_date=datetime(2026, 4, 25, tzinfo=UTC),
    end_date=datetime(2026, 4, 26, tzinfo=UTC),
    formats=["parquet", "psql"],
    psql_dsn="host=localhost dbname=osm user=osm",
)
try:
    result = run(cfg)  # OSM credentials picked up from OSM_USERNAME / OSM_PASSWORD
except OsmsgError as exc:
    ...

print(result["files"]["parquet"])  # → 'nepal.parquet'
print(result["rows"])              # → user count
```

Query a stored database (DuckDB file or Postgres) without re-running:

```python
from osmsg import connect, user_stats, daily_summary

conn = connect("nepal.duckdb")
top_10 = user_stats(conn, top_n=10)
days   = daily_summary(conn)
```

Typed exceptions (`OsmsgError` base, plus `UnknownRegionError` / `CredentialsRequiredError` / `GeofabrikAuthError` / `NoDataFoundError`) are catchable by callers; the CLI maps them to exit codes (2 = config/auth, 1 = no data). The package ships a `py.typed` marker for `mypy` / `ty`.

## Output formats

`-f parquet` (default) `-f csv` `-f json` `-f markdown` `-f psql`

Every run writes a portable `<name>.duckdb` (queryable with the duckdb CLI) plus the formats you asked for. Parquet is the canonical exchange format — open it directly with DuckDB / polars / pandas.

```sql
-- Query the duckdb file from anywhere
duckdb stats.duckdb -c "SELECT name, map_changes FROM (
  SELECT u.username AS name, SUM(s.nodes_created+s.ways_created) AS map_changes
  FROM users u JOIN changeset_stats s USING (uid) GROUP BY 1 ORDER BY 2 DESC LIMIT 10
)"
```

## Credentials

`--country` and Geofabrik internal URLs need OSM credentials (OAuth 2.0; OAuth 1.0a was retired June 2024). Public planet replication (`--url minute|hour|day`) needs none. Resolution order:

1. `--username` (CLI) + `--password-stdin` (one line on stdin), or `osm_username` / `osm_password` (`RunConfig`)
2. `OSM_USERNAME` / `OSM_PASSWORD` env vars (auto-loaded from `.env`)
3. Interactive `getpass` prompt — TTY only. Headless library callers raise `CredentialsRequiredError`.

Passwords are not accepted as a plain CLI flag — they would leak into shell history and `ps` output.

## Schema

Four tables, identical in DuckDB and PostgreSQL — write once, query anywhere.

| Table | Purpose |
|---|---|
| `users` | uid → username |
| `changesets` | metadata: hashtags, editor, bbox |
| `changeset_stats` | flat counts + nested `tag_stats` JSON |
| `state` | resume marker — exactly one row per source_url (UPSERTed each run) |

`map_changes` is computed at query time (sum of nine integer columns). See [docs/Stats.md](./docs/Stats.md) for the full methodology.

## Developer setup

```bash
git clone https://github.com/osgeonepal/osmsg && cd osmsg
uv sync
uv run pytest -m "not network"
uv run osmsg --help
```

See [docs/Manual.md](./docs/Manual.md) for the flag reference and [docs/Installation.md](./docs/Installation.md) for environment notes.
