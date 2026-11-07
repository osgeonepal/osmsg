# Installation

## End user

Use one of these: `uvx` runs osmsg once without installing it, `pip install` adds it to an environment,
and `uv tool install` installs the standalone CLI.

```bash
uvx --from osmsg osmsg --last hour
pip install osmsg
uv tool install osmsg
```

Wheels include the compiled `pyosmium` extension, so system OSM tools are not required.

`uvx` (from [uv](https://docs.astral.sh/uv/)) runs osmsg in a managed, throwaway environment. It suits
ad-hoc runs, CI jobs, and cron entries where you don't want to manage a venv. If a stale resolver cache
ever picks an older release, add `--refresh`:

```bash
uvx --refresh --from osmsg osmsg --last hour
```

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
just setup
just test
```

`just setup` runs `uv sync --all-groups`, which installs the runtime dependencies, the dev tools (`ruff`,
`ty`, `pytest`, `pre-commit`, `commitizen`), and the `api` group that the API tests import, then installs
the pre-commit, commit-msg, and pre-push git hooks. `just test` runs the offline tests.

### Pre-commit hooks

On commit: the `pre-commit-hooks` file checks (private keys, end of file, trailing whitespace, merge
conflicts, TOML, YAML, JSON, byte-order marks), `ruff` (lint + format), `uv-lock` (keeps `uv.lock` in
sync with `pyproject.toml`), and `ty` (Astral type checker). On the commit message: `commitizen`
(conventional commits). On push: a `pytest` hook runs the offline tests.

Hooks run automatically on every `git commit` once `just setup` has installed them. To run them by hand,
the commands below run every hook against the whole repo (the same as `just lint`), run a single hook,
run the hooks against specific files, and bump the hook versions in `.pre-commit-config.yaml`:

```bash
uv run pre-commit run --all-files
uv run pre-commit run ruff --all-files
uv run pre-commit run --files osmsg/cli.py
uv run pre-commit autoupdate
```

If a hook auto-fixes a file (e.g. `ruff` reformats, `uv-lock` refreshes the lockfile), the commit is aborted:
re-stage the changes and commit again.

### Tests

- `just test` (`pytest -m "not network"`) for offline unit tests (handlers, queries, exporters, CLI).
- `uv run pytest -m network` for integration tests against Geofabrik / OSM (requires `OSM_USERNAME` / `OSM_PASSWORD`).
