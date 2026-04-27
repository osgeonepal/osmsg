# Manual

## Time range

```bash
osmsg --last hour|day|week|month|year
osmsg --days 7
osmsg --start "2026-04-01 00:00:00" --end "2026-04-08 00:00:00"
osmsg --update                       # resume from last finished run in <name>.duckdb
```

> Times are UTC.

## Source

```bash
osmsg --url minute|hour|day          # planet replication shortcuts
osmsg --url https://...              # any OSM replication base
osmsg --country nepal --country india --country africa   # Geofabrik regions, resolved live
```

## Filters

```bash
osmsg --hashtags hotosm-project-1234 --hashtags mapathon
osmsg --hashtags mapathon --exact-lookup       # match whole hashtag, not substring
osmsg --users alice --users bob
osmsg --boundary region.geojson
```

> Each `--users`, `--hashtags`, `--tags`, `--length`, `--country`, `--url`, `-f`
> takes one value at a time; pass the flag again for additional values.

> Editor stats are always included when `--changeset` or `--hashtags` is on:
> the `editors` column lists every `created_by` tag the user appeared with.

## Tag stats

```bash
osmsg --tags building --tags highway           # per-key create/modify counts
osmsg --length highway --length waterway       # length in metres for created ways
osmsg --all-tags                               # every tag key
osmsg --all-tags --key-value                   # also key=value combos
```

## Output

```bash
osmsg --last day -f parquet                    # default; one columnar file
osmsg --last day -f csv -f json -f markdown
osmsg --last day --summary                     # daily breakdown in each requested format
osmsg --last day -f psql --psql-dsn "host=localhost dbname=osm user=osm"
```

> Every run writes `<name>.duckdb` plus the formats you ask for. Parquet is the canonical exchange — open with DuckDB, polars, or pandas directly.

> `--summary` follows the same `-f` formats: requesting `-f csv --summary` produces both `<name>.csv` and `<name>_summary.csv`. The `psql` target is intentionally skipped for summary — the daily breakdown is just a query over the four base tables, so consumers derive it on demand instead of duplicating data.

## Config file

Long invocations are easier to maintain in YAML. Keys mirror the CLI flag names.

```bash
osmsg --config nepal.yaml                      # all flags from yaml
osmsg --config nepal.yaml --rows 50            # CLI overrides yaml
```

```yaml
# nepal.yaml
name: nepal_weekly
country: [nepal]
last: week
hashtags:
  - hotosm-project-1234
  - mapathon
users: [alice, bob, charlie]
tags: [building, highway]
formats: [parquet, markdown]
summary: true
update: true
```

## Caching

Downloaded `.osc.gz` files cache to a per-user dir (`~/Library/Caches/osmsg` on macOS, `~/.cache/osmsg` on Linux). Re-running the same range reuses them — no network needed. `--cache-dir` to relocate, `--delete-temp` to clean up after a run.

## Credentials

`--country` and any `geofabrik` URL need OSM credentials. Resolution order:

1. `--username` (CLI) + `OSM_PASSWORD` env var, or `--password-stdin` to pipe a password in (e.g. `cat secret | osmsg --password-stdin ...`)
2. `OSM_USERNAME` + `OSM_PASSWORD` env vars (auto-loaded from `.env`)
3. Interactive prompt (TTY only)

> The CLI does not accept `--password` directly — passwords on the command line leak into shell history and `ps` output. Use stdin or env vars.

## Recipes

```bash
# Daily Nepal stats with summary
osmsg --country nepal --last day --summary --tags building --tags highway

# Mapathon report (hashtag substring, last 6 days, with TM totals)
osmsg --hashtags smforst --days 6 --summary --tm-stats

# Full year of global stats to Postgres (incremental-friendly)
osmsg --start "2025-01-01 00:00:00" --end "2026-01-01 00:00:00" \
      --url day --all-tags -f parquet -f psql \
      --psql-dsn "host=localhost dbname=osm_stats user=osm"

# Cron / systemd: refresh Nepal nightly
osmsg --country nepal --update
```

> See [Stats.md](./Stats.md) for how each field is computed.
