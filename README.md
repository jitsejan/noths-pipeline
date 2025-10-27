# noths-pipeline

Reviews-first Feefo data pipeline for NOTHS, with product and rating enrichment using DLT, dbt & Dagster.

## Quickstart
```bash
uv sync
make install
```

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