# syntax=docker/dockerfile:1.7

# ── Stage 1: build the wheel + venv with uv ────────────────────────────────────
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependency layer — cached on uv.lock unchanged.
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Project layer.
COPY osmsg /app/osmsg
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# ── Stage 2: slim Python 3.12 runtime ──────────────────────────────────────────
# pyosmium ships manylinux/musllinux wheels with libosmium statically linked, so
# we don't need apt-installed osmium-tool here.  We use python:3.12-slim instead
# of gcr.io/distroless/python3 because distroless still defaults to Python 3.11.
FROM python:3.12-slim-bookworm AS runtime

# Minimal runtime libs:
#   ca-certificates → HTTPS to planet/geofabrik/OSM
#   libexpat1       → pyosmium's XML parser (libosmium links to it dynamically)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    libexpat1 \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user.
RUN groupadd --system --gid 1000 osm \
    && useradd  --system --gid osm --uid 1000 --create-home --home-dir /home/osm osm

WORKDIR /work
COPY --from=builder --chown=osm:osm /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER osm
ENTRYPOINT ["osmsg"]
CMD ["--help"]
