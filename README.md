# osmsg

[![CI](https://github.com/osgeonepal/osmsg/actions/workflows/ci.yml/badge.svg)](https://github.com/osgeonepal/osmsg/actions/workflows/ci.yml)
[![Docker](https://github.com/osgeonepal/osmsg/actions/workflows/docker.yml/badge.svg)](https://github.com/osgeonepal/osmsg/actions/workflows/docker.yml)
[![PyPI](https://img.shields.io/pypi/v/osmsg.svg)](https://pypi.org/project/osmsg/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Container](https://img.shields.io/badge/ghcr.io-osgeonepal%2Fosmsg-2496ED?logo=docker)](https://github.com/osgeonepal/osmsg/pkgs/container/osmsg)

**OpenStreetMap Stats Generator.** A tiny CLI (and Python library) that turns OSM history into per-user counts
of nodes, ways, and relations created, modified, or deleted, written to parquet, csv, json, markdown, or Postgres.

A Project of [OSGeo Nepal](https://osgeonepal.org).

## What you get

- Per-user create/modify/delete counts over any time window.
- Tag and hashtag breakdowns (e.g. `building`, `#hotosm`).
- Country and custom-boundary filters via Geofabrik.
- Cron-friendly resume with `--update`.
- Outputs you can query: parquet, csv, json, markdown, DuckDB, Postgres.

## Install

Pick the one that fits how you work.

```bash
uvx --from osmsg osmsg --last hour       # zero-install, one-shot run
pip install osmsg                        # into your project
uv tool install osmsg                    # standalone CLI
docker run --rm -v "$PWD:/work" -w /work ghcr.io/osgeonepal/osmsg:latest --last hour
```

`uvx` can run osmsg in a throwaway environment , no install, no virtualenv to manage. Works
with any flag combination, e.g. `uvx --from osmsg osmsg --last hour --tags building --summary -f parquet -f markdown`.

## Quick start

```bash
osmsg --last hour                        # planet, last hour
osmsg --last day --tags building         # last day with a tag breakdown
osmsg --hashtags hotosm --last day       # only changesets tagged #hotosm
```

That's it. A `stats.duckdb` and a `stats.parquet` show up in your current folder.

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

```bash
osmsg --country nepal --update           # picks up where the last run stopped
```

Drop that into cron or a GitHub Actions schedule. State is stored inside the DuckDB file, so reruns are safe.

### 4. Query the output

```bash
duckdb stats.duckdb -c "SELECT username, SUM(nodes_created) AS n
                        FROM users JOIN changeset_stats USING (uid)
                        GROUP BY username ORDER BY n DESC LIMIT 10"
```

Same schema in DuckDB and Postgres: `users`, `changesets`, `changeset_stats`, `state`.

### 5. Use it as a library

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

Same pipeline as the CLI.

### 6. Long flag lists? Use a config

```bash
osmsg --config nepal.yaml
```

Any flag works as a YAML key. See [docs/Manual.md](./docs/Manual.md) for the full list.

## Output formats

Every run writes `stats.duckdb` (or `<--name>.duckdb`) plus the formats you ask for via
`-f parquet|csv|json|markdown|psql`. Parquet is the default. Open it with duckdb, polars, pandas, anything.

## Documentation

- [Installation](./docs/Installation.md)
- [Manual](./docs/Manual.md) (every flag, with examples)
- [Version control / release notes](./docs/Version_control.md)

## Contributing

Pull requests are welcome. Quick path:

```bash
git clone https://github.com/osgeonepal/osmsg && cd osmsg
git switch develop
uv sync
uv run pre-commit install
uv run pytest -m "not network"
```

Please read [CONTRIBUTING.md](./CONTRIBUTING.md) and the [Code of Conduct](./CODE_OF_CONDUCT.md) before opening a PR.
Use [Conventional Commits](https://www.conventionalcommits.org/) (`cz commit`).

## License

[MIT](./LICENSE) © OSGeo Nepal contributors.
