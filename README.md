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

## Data layers
Data stored in `feefo_pipeline.duckdb`:
- `bronze.*` - Raw DLT ingestion (feefo_reviews, feefo_products_for_reviews)
- `silver.*` - Staging views (stg_feefo_reviews, stg_feefo_product_ratings)
- `gold.*` - Business aggregates (product_summary)

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
