# Contributing

Contributions are welcome. Please read the [Code of Conduct](./CODE_OF_CONDUCT.md) before starting.

## Setup

```bash
git clone https://github.com/osgeonepal/osmsg && cd osmsg
git switch develop
uv sync
uv run pre-commit install
uv run pytest -m "not network"
```

## Workflow

1. Open an issue first for non-trivial changes.
2. Branch from `develop` (e.g. `fix/short-description`, `feat/short-description`).
3. Keep each PR to a single logical change. Squash intermediate commits before opening the PR.
4. Update the README or `docs/` for any user-visible behaviour change.

## Coding standards

- **Format + lint**: `ruff` (config in `pyproject.toml`). Pre-commit auto-fixes.
- **Type-check**: `ty` (Astral). Must pass with zero errors.
- **Tests**: `pytest -m "not network"` for offline checks; `pytest -m network` for live Geofabrik / OSM integration.
- **Commits**: [Conventional Commits](https://www.conventionalcommits.org/) via `cz commit`. See [docs/Version_control.md](./docs/Version_control.md).

## Releases

`cz bump` updates `pyproject.toml` + `osmsg/__version__.py`, refreshes `CHANGELOG.md`, and tags the release. Pushing the tag triggers PyPI publish + GHCR docker build via GitHub Actions.
