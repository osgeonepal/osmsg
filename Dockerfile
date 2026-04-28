# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev --no-editable

COPY osmsg /app/osmsg
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

RUN find /app/.venv -type d -name __pycache__ -exec rm -rf {} + \
    && sed -i 's|^home = .*|home = /usr/bin|' /app/.venv/pyvenv.cfg \
    && rm -f /app/.venv/bin/python /app/.venv/bin/python3 /app/.venv/bin/python3.13 \
    && ln -s /usr/bin/python3.13 /app/.venv/bin/python3.13 \
    && ln -s python3.13 /app/.venv/bin/python3 \
    && ln -s python3.13 /app/.venv/bin/python

FROM gcr.io/distroless/python3-debian13:nonroot AS runtime

WORKDIR /work
COPY --from=builder --chown=nonroot:nonroot /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/.venv/bin/osmsg"]
CMD ["--help"]
