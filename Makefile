.PHONY: install lint format test dbt dbt-test dagster clean run run-dev run-full clean-data

install:
	uv sync --all-extras

lint:
	uv run ruff check .

format:
	uv run ruff format .

test:
	uv run pytest -q

dbt:
	cd dbt && uv run dbt run

dbt-test:
	cd dbt && uv run dbt test

run:
	uv run python -m pipeline.cli run --max-pages 1 --mode merge

run-dev:
	uv run python -m pipeline.cli run --max-pages 5 --mode merge

run-full:
	uv run python -m pipeline.cli run --max-pages 100 --mode merge

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage

clean-data:
	rm -f feefo_pipeline.duckdb
	rm -rf .dlt/pipelines/