# osmsg

Generate OpenStreetMap user stats from the command line. Point it at a time window, get back per-user counts of nodes/ways/relations created, modified, and deleted, in parquet, csv, json, markdown, or straight into Postgres.

## Install

```bash
pip install osmsg
# or, as a standalone CLI
uv tool install osmsg
# or, no install
docker run --rm -v "$PWD:/work" -w /work ghcr.io/osgeonepal/osmsg:latest --last hour
```

## Examples

```bash
# What happened in the last hour, planet-wide
osmsg --last hour

# Yesterday's stats for a country (needs OSM credentials, see below)
osmsg --country nepal --last day

# Custom range, with per-key tag breakdowns and a daily summary
osmsg --start "2026-04-01" --end "2026-04-08" \
      --tags building --tags highway --summary

# Only changesets tagged #hotosm (substring by default; --exact-lookup for whole-word)
osmsg --hashtags hotosm --last day

# Cron-friendly: pick up where the last run left off
osmsg --country nepal --update
```

YAML configs work too if your flag list gets long: `osmsg --config nepal.yaml`.

## Output

Every run writes `stats.duckdb` (or `<--name>.duckdb`) plus whatever formats you ask for via `-f parquet|csv|json|markdown|psql`. Parquet is the default. Open it with duckdb, polars, pandas, whatever.

```bash
duckdb stats.duckdb -c "SELECT username, SUM(nodes_created) AS n
                        FROM users JOIN changeset_stats USING (uid)
                        GROUP BY username ORDER BY n DESC LIMIT 10"
```

The schema is the same in DuckDB and Postgres. Four tables: `users`, `changesets`, `changeset_stats`, and `state` (the resume marker for `--update`).

## Credentials

`--country` (and Geofabrik URLs) need an OSM account; public planet replication (`--url minute|hour|day`) doesn't.

Set `OSM_USERNAME` and `OSM_PASSWORD` in your environment or a `.env` file. Or pass `--username` and pipe the password to `--password-stdin`. OAuth 2.0 happens behind the scenes.

## Library

```python
from datetime import datetime, UTC
from osmsg import RunConfig, run

result = run(RunConfig(
    name="nepal",
    countries=["nepal"],
    start_date=datetime(2026, 4, 25, tzinfo=UTC),
    end_date=datetime(2026, 4, 26, tzinfo=UTC),
))
print(result["files"]["parquet"])
```

That's the same pipeline the CLI runs. See [docs/Manual.md](./docs/Manual.md) for everything else.

## Develop

```bash
git clone https://github.com/osgeonepal/osmsg && cd osmsg
uv sync
uv run pytest
uv run osmsg --help
```
