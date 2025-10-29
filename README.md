# noths-pipeline

Reviews-first Feefo data pipeline for NOTHS, with product and rating enrichment using DLT, dbt & Dagster.

## Quickstart
```bash
uv sync
make install
```

## Running the pipeline
```bash
# 1. Ingest raw data (DLT)
make run          # Quick test (1 page)
make run-dev      # Development (5 pages)
make run-full     # Full load (100 pages)

# 2. Transform data (dbt)
make dbt          # Build silver + gold tables
make dbt-test     # Run dbt tests

# Full pipeline
make clean-data && make run && make dbt
```

## Code quality

### Pre-commit hooks
Install pre-commit hooks to automatically run linting, type checking, and tests before each commit:

```bash
make pre-commit-install
```

The following hooks will run on commit:
- **ruff** - Python linter and formatter
- **mypy** - Static type checker
- **sqlfluff** - SQL linter with dbt templating support
- **pytest** - Python unit tests (runs on every commit)
- **dbt test** - dbt data tests (runs when dbt files change)

Run all hooks manually without committing:
```bash
make pre-commit-run
```

Run linting and formatting separately:
```bash
make lint      # Check code with ruff + mypy
make format    # Auto-fix formatting with ruff
```

Skip hooks for a specific commit (use sparingly):
```bash
git commit --no-verify -m "message"
```

## Running inside Docker
Build the image (uses the lock file for deterministic installs):

```bash
docker build -t noths-pipeline .
```

Run the full ingestion + transformation flow:

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -e MAX_PAGES=5 \
  noths-pipeline
```

Using the bundled make targets:

```bash
make docker-build
make docker-run DOCKER_RUN_ARGS="-e MAX_PAGES=5"
```

Environment variables recognised by the container:
- `MERCHANT_ID` (default `notonthehighstreet-com`)
- `MAX_PAGES` (default `1`)
- `MODE` (default `merge`)
- `PERIOD_DAYS`, `SINCE`, `UNTIL`
- `INCLUDE_RATINGS` (`1` to include ratings, `0` to skip)
- `DUCKDB_PATH` (default `/app/data/feefo_pipeline.duckdb`)

Mount `$(pwd)/data` (as shown above) to persist the DuckDB output on the host.

When the container finishes, it will display the top five products by review count using the `gold.product_summary` table so you can immediately see the results.

## Data layers
Data stored in `feefo_pipeline.duckdb`:
- `bronze.*` - Raw DLT ingestion (feefo_reviews, feefo_products_for_reviews)
- `silver.*` - Staging views (stg_feefo_reviews, stg_feefo_product_ratings)
- `gold.*` - Business aggregates (product_summary)

## Design choices
- `dlt` handles pagination, deduping, and incremental loading out of the box, so ingestion code stays tiny.
- `dbt` keeps transformations declarative and reviewable for both analysts and engineers.
- `DuckDB` is a fast, portable analytical store that works locally without extra services.
- `pytest` (with parametrisation) gives terse, expressive tests compared to the heavier `unittest` workflow.
- `pre-commit` wires linting, typing, and data tests into a single command for consistent CI parity.
- Medallion layering (bronze → silver → gold) makes the pipeline easy to navigate and extend.
- `uv` provides deterministic Python environments and dependency resolution faster than pip or poetry.

## Project structure
- data/         # local DuckDB file
- dbt/          # SQL models and transformations
- pipeline/     # Python ingestion + orchestration
- tests/        # pytest suite


#### Confirm Python env
Make sure uv recognized your `.python-version`:
```bash
uv python pin 3.13
uv run python --version  # should print Python 3.13.x
```
