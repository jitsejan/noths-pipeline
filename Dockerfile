# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:python3.11-bookworm AS base

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv

# Install Python dependencies using the existing lock file for reproducibility
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy project source
COPY . .

# Ensure helper script is executable
RUN chmod +x scripts/run_pipeline.sh

# Activate runtime defaults
ENV APP_ROOT="/app" \
    DBT_PROFILES_DIR="/app/dbt" \
    DUCKDB_PATH="/app/data/feefo_pipeline.duckdb"

# Sanity check that critical dependencies are available
RUN uv run --frozen --no-dev python -c "import dlt, dbt" >/dev/null

ENTRYPOINT ["/app/scripts/run_pipeline.sh"]
