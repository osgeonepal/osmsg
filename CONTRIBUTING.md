# Contributing

Thanks for considering a contribution to **osmsg**. This is an [OSGeo Nepal](https://osgeonepal.org) project and we
welcome PRs of every size: a typo fix, a new flag, a perf patch, a docs cleanup. Please read the
[Code of Conduct](./CODE_OF_CONDUCT.md) before you start.

## Setup

```bash
git clone https://github.com/osgeonepal/osmsg && cd osmsg
git switch develop
just setup
just test
```

`just setup` runs `uv sync --all-groups`, which installs the runtime dependencies, the dev tools (`ruff`,
`ty`, `pytest`, `pre-commit`, `commitizen`), and the `api` group that the API tests import. It then
installs the pre-commit, commit-msg, and pre-push git hooks. `just test` runs the offline test suite
(`pytest -m "not network"`). The project needs `uv` and [`just`](https://github.com/casey/just);
system OSM libraries are not required.

If you do not have `uv` yet:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Workflow

1. Open an issue first for non-trivial changes so we can agree on the approach.
2. Branch from `develop` (e.g. `fix/short-description`, `feat/short-description`).
3. Keep each PR to a single logical change. Squash intermediate commits before opening the PR.
4. Update the README or `docs/` for any user-visible behaviour change.
5. Open the PR against `develop`. `master` is reserved for releases.

## Coding standards

- **Format and lint**: `ruff` (config in `pyproject.toml`). Pre-commit auto-fixes most issues. `just lint` runs
  every hook against the whole repo.
- **Type-check**: `ty` (Astral). Must pass with zero errors: `uv run ty check osmsg`.
- **Tests**:
  - `just test` for offline checks.
  - `uv run pytest -m network` for live Geofabrik / OSM integration (needs `OSM_USERNAME` and `OSM_PASSWORD`).
  - The API speed and correctness tests run against the deployment named by `OSMSG_API_BASE` and skip
    when it is unset.
- **Commits**: [Conventional Commits](https://www.conventionalcommits.org/) via `uv run cz commit`. See [docs/Version_control.md](./docs/Version_control.md).

## CI

Every PR runs:

- `ruff check`, `ruff format --check`, `ty check`, `pytest -m "not network"` ([ci.yml](./.github/workflows/ci.yml))
- Wheel and sdist build ([ci.yml](./.github/workflows/ci.yml))
- Smoke run of `osmsg --last hour` to catch regressions on real planet data ([ci.yml](./.github/workflows/ci.yml))
- Multi-arch Docker build ([docker.yml](./.github/workflows/docker.yml))

A green CI is a hard requirement before merge.

## Releases

Releases are cut from `master` by maintainers using `commitizen`:

```bash
uv run cz bump
git push --follow-tags
```

`cz bump` updates the version in `pyproject.toml` and `osmsg/__version__.py`, refreshes `CHANGELOG.md`, and tags
the release. Pushing the tag (or publishing a GitHub Release) triggers:

- PyPI publish via [release.yml](./.github/workflows/release.yml) using the `PYPI_API_TOKEN` repo secret.
- Multi-arch Docker image build to `ghcr.io/osgeonepal/osmsg` via [docker.yml](./.github/workflows/docker.yml).

## Reporting issues

Bugs and feature requests live in [GitHub issues](https://github.com/osgeonepal/osmsg/issues). For bugs, please include:

- The `osmsg --version`, OS, and Python version.
- The exact command (or YAML config) you ran.
- The full traceback or error output.

## License

By contributing, you agree your contributions will be licensed under the [MIT License](./LICENSE).
