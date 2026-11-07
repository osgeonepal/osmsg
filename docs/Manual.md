# Manual

The full flag reference, grouped by task. For a first run, see the
[README quick start](../README.md#quick-start).

## Time range

A run covers a relative period (`--last`, `--days`) or explicit `--start`/`--end` timestamps.
`--update` resumes from the last finished run recorded in `<name>.duckdb`; how it picks the replication
granularity is described in [Setting up a store](#setting-up-a-store).

```bash
osmsg --last hour|day|week|month|year
osmsg --days 7
osmsg --start "2026-04-01 00:00:00" --end "2026-04-08 00:00:00"
osmsg --update
```

> Times are UTC.

## Source

`--url` takes a planet replication shortcut (`minute`, `hour`, `day`) or the base URL of any OSM
replication stream. `--country` takes Geofabrik regions, resolved live.

```bash
osmsg --url minute|hour|day
osmsg --url https://...
osmsg --country nepal --country india --country africa
```

> When `--url` is omitted, osmsg picks a planet replication granularity that fits the requested
> span: minute for spans under 6h, hour for 6h to 7d, day for 7d or more. A warning prints when the
> auto-switch happens; pass `--url` explicitly to override (also suppressed by `--country`,
> `--update`, or multiple `--url` values).

## Filters

`--hashtags` matches by substring; `--exact-lookup` matches the whole hashtag. `--boundary` takes a
Geofabrik region name, a path to a GeoJSON file, or an inline GeoJSON string.

```bash
osmsg --hashtags hotosm-project-1234 --hashtags mapathon
osmsg --hashtags mapathon --exact-lookup
osmsg --users alice --users bob
osmsg --boundary nepal
osmsg --boundary region.geojson
osmsg --boundary '{"type":"Polygon",...}'
```

> `--boundary` filters changesets whose bounding box intersects the given geometry.
> A Geofabrik region name resolves from the same index as `--country`, without a separate file.
> `--boundary` only filters; it does not change the replication source.
> To scope the replication source to a country's diffs, use `--country` instead.
>
> Each `--users`, `--hashtags`, `--tags`, `--length`, `--country`, `--url`, `-f`
> takes one value at a time; pass the flag again for additional values.
>
> Editor stats are always included when `--changeset` or `--hashtags` is on:
> the `editors` column lists every `created_by` tag the user appeared with.

## Tag stats

`--tags` adds per-key create and modify counts. `--length` adds the length in metres of created ways
for the given keys, a lower bound (see [length_backfill.md](./length_backfill.md)). `--keys` counts every
tag key without a value breakdown, and `--all` counts every `key=value` combination and adds changeset
metadata (hashtags, editors).

```bash
osmsg --tags building --tags highway
osmsg --length highway --length waterway
osmsg --keys
osmsg --all
```

## Output

Parquet is the default format and writes one columnar file. `--summary` adds a daily breakdown in each
requested format.

```bash
osmsg --last day -f parquet
osmsg --last day -f csv -f json -f markdown
osmsg --last day --summary
osmsg --last day -f psql --psql-dsn "host=localhost dbname=osm user=osm"
```

> Every run writes `<name>.duckdb` plus the formats you ask for. Parquet is the canonical exchange:
> open with DuckDB, polars, or pandas directly.
>
> `--summary` follows the same `-f` formats: requesting `-f csv --summary` produces both `<name>.csv`
> and `<name>_summary.csv`. The `psql` target is intentionally skipped for summary, since the daily
> breakdown is a query over `changeset_stats` and `changesets`, so consumers derive it on demand.

`<name>.duckdb` is stamped with the query that built it. Rerunning the same query with a different `-f`
re-exports straight from that store, so adding a format skips the download and parse. Changing any query parameter
(window, hashtags, tags, boundary) recomputes; `--overwrite` forces a fresh recompute.

## Config file

Long invocations are easier to maintain in YAML. Each key is the option's underscore name (the flag
with `-` turned to `_`, so `output_dir`, `history_url`, `psql_dsn`); `--all` is `all_stats` and
`--keys` is `keys_only`. A key that doesn't match an option is ignored.

A flag given on the command line overrides the same key in the YAML file. The YAML
example below is `nepal.yaml`.

```bash
osmsg --config nepal.yaml
osmsg --config nepal.yaml --rows 50
```

```yaml
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

Downloaded `.osc.gz` files cache to a per-user dir (`~/Library/Caches/osmsg` on macOS,
`~/.cache/osmsg` on Linux). Re-running the same range reuses them without downloading again.
`--cache-dir` relocates the cache and `--delete-temp` cleans it up after a run.

## Setting up a store

`--insert` loads history into the store and seeds the resume position, then exits. Follow it with
`--update` to catch up to now and keep current. DuckDB is the default store; pass `--psql-dsn` to use
Postgres (it implies `-f psql`).

The first pair loads all published history into `stats.duckdb` and then catches up to now (the
`--update` line is the one to run on cron). The last three load into Postgres, load a slice, and load
from local planet files.

```bash
osmsg --insert
osmsg --update

osmsg --insert --psql-dsn "host=localhost dbname=osm user=osm"
osmsg --insert --start 2020-01-01 --end 2023-01-01
osmsg --insert --osh-file history.osh.pbf --changeset-file changesets.osm.bz2
```

- Without a window it loads the whole dataset; `--start/--end` loads a slice and resumes from the slice end.
- `--osh-file` with `--changeset-file` converts local planet files into the store (offline, or a custom
  extract). Give both together.
- The Postgres load uses the bulk path (drops indexes and keys, rebuilds after).

`--insert` and `--update` pick the replication granularity from how far behind the store is. A fresh
store clears the multi-week backlog on day diffs (tens of files), then refines to hour and minute as it
stays current. For near-real-time, run `osmsg --update --url minute`. A store tracks one granularity at
a time. Without `--url`, `--update` only moves to a finer granularity; with `--url`, it switches to the
given one. Either way the new source starts at the last timestamp the old source processed, and the old
source's resume state is removed, so the windows stay disjoint and nothing is counted twice. Pass
`--url` to either command to set the granularity yourself.

## Cloud-native history

Months covered by a published parquet dataset (default `kshitijrajsharma/osmsg-history` on
HuggingFace) are read remotely. The recent uncovered tail uses the live replication path. This is on
by default.

The first command reads entirely from the dataset, the second reads covered months remotely and the
current month live, and the third uses only the live path.

```bash
osmsg --start 2015-01-01 --end 2020-01-01
osmsg --start 2024-01-01
osmsg --last week --no-history
```

- `--no-history` (env `OSMSG_HISTORY=0`) uses the live path.
- `--history-url` (env `OSMSG_HISTORY_URL`) sets the dataset location.
- The live path is used when the dataset is unreachable, with `--update`, and with `--length`.

### Postgres as a source of truth

`osmsg --insert --psql-dsn ...` loads the dataset into osmsg's schema and seeds the resume position;
`osmsg --update --psql-dsn ...` then keeps Postgres current. `--psql-bulk` (env `OSMSG_PSQL_BULK`)
forces the bulk path on a plain run; `--insert` already uses it.

## Maintaining the dataset

`osmsg maintain` builds and publishes the history parquet. The commands below build one finished month
and upload it, build a month locally for review, write and upload `manifest.json`, and convert local
planet files.

```bash
osmsg maintain month 2026-06 --repo osgeonepal/osmsg-history
osmsg maintain month 2026-06 --no-upload
osmsg maintain publish out --repo osgeonepal/osmsg-history
osmsg maintain convert history.osh.pbf changesets.osm.bz2 2005-01-01 2026-06-01 work --parts 24
```

`month` builds from the live day diffs, exports the two partitions, uploads, and advances the
manifest. It refuses to publish a month whose data stops short of the month boundary (pass
`--allow-incomplete` to override), so published months are complete by construction. Re-running
`osmsg maintain month <YYYY-MM>` rebuilds a month and overwrites its published partition, which repairs
a month that was first generated from a mid-day planet snapshot. `convert` turns local planet files
into the datasets out of core. Uploads use the `hf` CLI (`uvx`), so be logged in to HuggingFace.

## Credentials

`--country` and any `geofabrik` URL need OSM credentials. Resolution order:

1. `--username` (CLI) + `OSM_PASSWORD` env var, or `--password-stdin` to pipe a password in
   (e.g. `cat secret | osmsg --password-stdin ...`)
2. `OSM_USERNAME` + `OSM_PASSWORD` env vars (auto-loaded from `.env`)
3. Interactive prompt (TTY only)

> The CLI does not accept `--password` directly, because passwords on the command line leak into
> shell history and `ps` output. Use stdin or env vars.

## Recipes

Daily Nepal stats with a summary:

```bash
osmsg --country nepal --last day --summary --tags building --tags highway
```

A mapathon report over the last six days, matching the hashtag by substring and adding Tasking Manager
totals:

```bash
osmsg --hashtags smforst --days 6 --summary --tm-stats
```

A full year of global stats into Postgres:

```bash
osmsg --start "2025-01-01 00:00:00" --end "2026-01-01 00:00:00" \
      --url day --all -f parquet -f psql \
      --psql-dsn "host=localhost dbname=osm_stats user=osm"
```

All-time Nepal stats from planet day diffs filtered by the Nepal boundary, since Geofabrik keeps about
four months of diffs per country:

```bash
osmsg --url day --boundary nepal --start "2012-09-13" -f parquet -f psql ...
```

A nightly refresh of Nepal from cron or a systemd timer:

```bash
osmsg --country nepal --update
```

> `map_changes` per row is the sum of the nine element columns
> (`{nodes,ways,rels}_{created,modified,deleted}`); POI counters are tracked separately.
