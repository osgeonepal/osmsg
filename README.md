# osmsg

[![CI](https://github.com/osgeonepal/osmsg/actions/workflows/ci.yml/badge.svg)](https://github.com/osgeonepal/osmsg/actions/workflows/ci.yml)
[![Docker](https://github.com/osgeonepal/osmsg/actions/workflows/docker.yml/badge.svg)](https://github.com/osgeonepal/osmsg/actions/workflows/docker.yml)
[![PyPI](https://img.shields.io/pypi/v/osmsg.svg)](https://pypi.org/project/osmsg/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Container](https://img.shields.io/badge/ghcr.io-osgeonepal%2Fosmsg-2496ED?logo=docker)](https://github.com/osgeonepal/osmsg/pkgs/container/osmsg)

**OpenStreetMap Stats Generator.** A CLI and Python library that turns OSM history into per-user counts
of nodes, ways, and relations created, modified, or deleted, written to parquet, csv, json, markdown, or Postgres.

A Project of [OSGeo Nepal](https://osgeonepal.org).

## Features

- Per-user create/modify/delete counts over any time window.
- Tag and hashtag breakdowns (e.g. `building`, `#hotosm`).
- Country and custom-boundary filters via Geofabrik.
- Cron-friendly resume with `--update`.
- One-command setup: `osmsg --insert` loads all history into your store, `osmsg --update` keeps it current.
- Outputs you can query: parquet, csv, json, markdown, DuckDB, Postgres.
- Cloud-native history: months covered by a published parquet dataset are read remotely.

## Install

Pick the one that fits how you work: `uvx` runs osmsg once without installing it, `pip install` adds it
to a project, `uv tool install` installs the standalone CLI, and the Docker image runs it in a container.

```bash
uvx --from osmsg osmsg --last hour
pip install osmsg
uv tool install osmsg
docker run --rm -v "$PWD:/work" -w /work ghcr.io/osgeonepal/osmsg:latest --last hour
```

`uvx` accepts any flag combination, for example `uvx --from osmsg osmsg --last hour --tags building --summary -f parquet -f markdown`.

Planned install channels (not yet published): conda-forge (`conda install -c conda-forge osmsg`) and a
Homebrew tap (`brew install osgeonepal/tap/osmsg`).

On Windows, download `osmsg.exe` from the [latest release](https://github.com/osgeonepal/osmsg/releases)
and double-click it to open the desktop app. Pick a Quick range (last hour, day, week, month, year, or
all time) or type your own dates, set the options, click Compute, and open the output folder. The CLI
below is for macOS, Linux, and pip/uv users.

## Quick start

The first command counts the whole planet over the last hour, the second adds a `building` tag
breakdown over the last day, and the third keeps only changesets tagged `#hotosm`:

```bash
osmsg --last hour
osmsg --last day --tags building
osmsg --hashtags hotosm --last day
```

Each run writes a `stats.duckdb` and a `stats.parquet` to the current folder.

## Set up a full history store

Two commands give you a complete, self-updating store. `--insert` loads all of OSM history from the
published dataset into `stats.duckdb`, records where to resume, and exits; `--update` catches up to now
and is the command to repeat on cron:

```bash
osmsg --insert
osmsg --update
```

`osmsg` clears the multi-week backlog on day diffs, then refines to finer diffs as the store stays
current. For near-real-time, run `osmsg --update --url minute`.

Pick your store with one flag. DuckDB is the default (`stats.duckdb`); add a DSN for Postgres:

```bash
osmsg --insert --psql-dsn "postgresql://user:pass@localhost/osmsg"
osmsg --update --psql-dsn "postgresql://user:pass@localhost/osmsg"
```

Load only a slice with `--start/--end`; `--update` then continues from the end of that slice:

```bash
osmsg --insert --start 2020-01-01 --end 2023-01-01
```

Already have the planet files? Insert from them directly:

```bash
osmsg --insert --osh-file history-latest.osh.pbf --changeset-file changesets-latest.osm.bz2
```

## Tutorials

### 1. Stats for a country

```bash
osmsg --country nepal --last day
```

`--country` resolves through Geofabrik and needs an OSM account. Set `OSM_USERNAME` and `OSM_PASSWORD`
in your shell or a `.env` file:

```bash
export OSM_USERNAME=you
export OSM_PASSWORD=secret
```

### 2. A custom date range with summaries

```bash
osmsg --start "2026-04-01" --end "2026-04-08" \
      --tags building --tags highway --summary
```

`--summary` adds a daily rollup file alongside the per-changeset stats.

### 3. Run on a schedule

`--update` picks up where the last run stopped:

```bash
osmsg --country nepal --update
```

Drop that into cron or a GitHub Actions schedule. State is stored inside the DuckDB file, so reruns are safe.

### 4. Query the output

```bash
duckdb stats.duckdb -c "SELECT username, SUM(nodes_created) AS n
                        FROM users JOIN changeset_stats USING (uid)
                        GROUP BY username ORDER BY n DESC LIMIT 10"
```

DuckDB and Postgres share the tables `users`, `changesets`, `changeset_stats`, and `state`. Postgres
stores the changeset bounding box as `min_lon`, `min_lat`, `max_lon`, `max_lat` columns (DuckDB uses a
`geom` column) and adds a `changeset_hashtag` table with one row per hashtag and changeset.

### 5. Run the API

Push stats into Postgres, then start the Litestar API. The API dependencies live in the `api` dependency
group and the `api/` package runs from a repo checkout (it is not part of the published wheel), so run it
from a clone:

```bash
git clone https://github.com/osgeonepal/osmsg && cd osmsg
uv sync --group api
uv run osmsg --last day --format psql --psql-dsn "postgresql://user:pass@localhost/osmsg"
uv run --group api litestar --app api.app:app run --host 0.0.0.0 --port 8000
```

```text
GET /health
GET /api/v2/hashtag/hotosm/summary
GET /api/v2/hashtag/hotosm/leaderboard
GET /docs
```

For self-hosting with Docker Compose and systemd, see [docs/infra.md](./docs/infra.md).

### 6. Use it as a library

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

`run` executes the same pipeline as the CLI, configured by the `RunConfig` fields. It raises
`OsmsgError` (or a subclass) on failure, and `NoDataFoundError` when the window produces no stats. `countries` needs OSM credentials, read from `OSM_USERNAME`
and `OSM_PASSWORD` or passed as `osm_username` and `osm_password`.

### 7. Use a config file

Long flag lists fit in a YAML file:

```bash
osmsg --config nepal.yaml
```

Each option is a YAML key written with its underscore name: `output_dir`, `history_url`, `all_stats`,
`formats`, `psql_dsn`, and so on (not the dashed flag). See [docs/Manual.md](./docs/Manual.md).

## Output formats

Every run writes `stats.duckdb` (or `<--name>.duckdb`) plus the formats you ask for via
`-f parquet|csv|json|markdown|psql`. Parquet is the default. DuckDB, polars, and pandas read it directly.

Rerunning the same query with a different `-f` re-exports from the existing `<name>.duckdb` without
refetching, so adding a format skips the download and parse. Pass `--overwrite` to force a fresh recompute.

## Configuration

Most flags have a matching `OSMSG_*` env var so the CLI, a `.env` file, and a
docker-compose `environment:` block all reach the same setting. CLI flag wins over env var.

| CLI flag | Env var | Default | Notes |
| --- | --- | --- | --- |
| `--name` | `OSMSG_NAME` | `stats` | Output basename; sets `<name>.duckdb`. |
| `--country` | `OSMSG_COUNTRY` | unset | Geofabrik region id(s). Comma-separated when set via env. |
| `--boundary` | `OSMSG_BOUNDARY` | unset | GeoJSON path or inline GeoJSON. |
| `--url` | `OSMSG_URL` | unset | `minute`/`hour`/`day` shortcut or full URL. Unset picks the granularity from the time span. Comma-separated when set via env. |
| `--workers` | `OSMSG_WORKERS` | cpu count | Parallel parse workers. |
| `--cache-dir` | `OSMSG_CACHE_DIR` | platform cache | Where downloaded OSM files are kept across runs. |
| `--output-dir` | `OSMSG_OUTPUT_DIR` | `.` | Where `<name>.duckdb` and exports are written. |
| `--format` / `-f` | `OSMSG_FORMAT` | `parquet` | Repeat for multiple. Comma-separated when set via env. |
| `--overwrite` | (none) | off | Recompute even if `<name>.duckdb` already holds this exact query. |
| `--psql-dsn` | `OSMSG_PSQL_DSN` | unset | libpq DSN for `-f psql`. |
| `--psql-bulk` | `OSMSG_PSQL_BULK` | off | Faster first full load to Postgres. |
| `--history` / `--no-history` | `OSMSG_HISTORY` | on | Read covered months from the published dataset. |
| `--history-url` | `OSMSG_HISTORY_URL` | `osmsg-history` | Published dataset location. |
| `--insert` | (none) | off | Load history into the store and seed resume, then exit. Without a window it loads all of it. |
| `--seed-only` | (none) | off | With `--insert`: seed resume state at the published frontier without loading rows. |
| `--osh-file` / `--changeset-file` | (none) | unset | Insert from local planet history + changeset files. |
| `--max-update-window-hours` | `OSMSG_MAX_UPDATE_WINDOW_HOURS` | unset | Cap each `--update` run to this many hours so a large backlog catches up over successive runs. |
| `--changeset-pad-hours` | `OSMSG_CHANGESET_PAD_HOURS` | `1` | See below. |
| (auto-bootstrap on `--update`) | `OSMSG_BOOTSTRAP` | `hour` | `hour`, `day`, or `week`. Used when `--update` runs against an empty DB. |
| (auto-bootstrap on `--update`) | `OSMSG_BOOTSTRAP_DAYS` | unset | Integer N; overrides `OSMSG_BOOTSTRAP`. |
| OSM credentials (Geofabrik) | `OSM_USERNAME`, `OSM_PASSWORD` | unset | Required only when a Geofabrik URL is in use. |

A `.env` file at the working directory is loaded automatically.

## Maintainers

Generating and publishing the history dataset is the `osmsg maintain` group. `month` builds and
uploads one finished month (with `--no-upload` it only builds it locally for review). `convert` turns
local planet files into the datasets, and `publish` writes and uploads the manifest. `refresh` pulls
the newest published month into a local artifact, `prune-pg` deletes Postgres rows that published
history covers, and `check --fix` backfills stub changesets whose metadata is missing.

```bash
osmsg maintain month 2026-06 --repo osgeonepal/osmsg-history
osmsg maintain month 2026-06 --no-upload
osmsg maintain convert history.osh.pbf changesets.osm.bz2 2005-01-01 2026-06-01 work --parts 24
osmsg maintain publish work/out --repo osgeonepal/osmsg-history
osmsg maintain refresh --artifact-dir /srv/osmsg/artifact
osmsg maintain prune-pg --psql-dsn "postgresql://user:pass@localhost/osmsg"
osmsg maintain check --psql-dsn "postgresql://user:pass@localhost/osmsg" --fix
```

## Documentation

- [Installation](./docs/Installation.md)
- [Manual](./docs/Manual.md) (every flag, with examples)
- [Self-hosting / Docker Compose](./docs/infra.md)
- [Roadmap](./ROADMAP.md)
- [Version control / release notes](./docs/Version_control.md)

## Contributing

Pull requests are welcome. `just setup` installs every dependency group and the git hooks, and
`just test` runs the offline test suite:

```bash
git clone https://github.com/osgeonepal/osmsg && cd osmsg
git switch develop
just setup
just test
```

Please read [CONTRIBUTING.md](./CONTRIBUTING.md) and the [Code of Conduct](./CODE_OF_CONDUCT.md) before opening a PR.
Use [Conventional Commits](https://www.conventionalcommits.org/) (`uv run cz commit`).

## License

[MIT](./LICENSE) © OSGeo Nepal contributors.
