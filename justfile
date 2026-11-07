set shell := ["bash", "-uc"]
set positional-arguments

default:
    @just --list

setup:
    uv sync --all-groups
    uv run pre-commit install --install-hooks --hook-type pre-commit --hook-type commit-msg --hook-type pre-push

lint:
    uv run pre-commit run --all-files

test *ARGS:
    uv run pytest -m "not network" "$@"

test-all *ARGS:
    uv run pytest "$@"

build:
    uv build --no-sources

run *ARGS:
    uv run osmsg "$@"
