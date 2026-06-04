set shell := ["bash", "-uc"]

default:
    @just --list

setup:
    uv sync --all-groups
    uv run pre-commit install --install-hooks --hook-type pre-commit --hook-type commit-msg

lint:
    uv run pre-commit run --all-files

test *ARGS:
    uv run pytest -m "not network" {{ARGS}}

test-all *ARGS:
    uv run pytest {{ARGS}}

build:
    uv build --no-sources
