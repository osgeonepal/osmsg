# Installation

## End user

```bash
pip install osmsg
# or
uv tool install osmsg
```

Wheels include the compiled `pyosmium` extension; no system OSM tools are required.

## Docker

Pull a published image from GHCR:

```bash
docker pull ghcr.io/osgeonepal/osmsg:latest
docker run --rm -v "$PWD:/work" -w /work ghcr.io/osgeonepal/osmsg:latest --last hour
```

Or build locally:

```bash
docker build -t osmsg:latest .
docker run --rm -v "$PWD:/work" -w /work osmsg --last hour
```

## Development

```bash
git clone https://github.com/osgeonepal/osmsg && cd osmsg
git switch develop
uv sync
uv run pre-commit install
uv run pytest -m "not network"
```

`uv sync` installs runtime + dev tools (`ruff`, `ty`, `pytest`, `pre-commit`, `commitizen`) from `pyproject.toml`.

### Pre-commit hooks

`ruff` (lint + format), `ty` (Astral type checker), `markdownlint`, `commitizen` (conventional commits).

### Tests

- `pytest -m "not network"` for offline unit tests (handlers, queries, exporters, CLI).
- `pytest -m network` for integration tests against Geofabrik / OSM (requires `OSM_USERNAME` / `OSM_PASSWORD`).
