# noths-pipeline

Reviews-first Feefo data pipeline for NOTHS, with product and rating enrichment using DLT, dbt & Dagster.

## Quickstart
```bash
uv sync
make install
```

## Running the pipeline
```bash
make run          # Quick test (1 page)
make run-dev      # Development (5 pages)
make run-full     # Full load (100 pages)
make clean-data   # Clean database
```

The pipeline fetches Feefo reviews and enriches them with product ratings for each SKU found in the reviews. Data is stored in `feefo_pipeline.duckdb` under the `bronze` schema.

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