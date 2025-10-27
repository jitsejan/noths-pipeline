.PHONY: install lint format test dbt dagster clean

install:
	uv sync --all-extras

lint:
	uv run ruff check .

format:
	uv run ruff format .

test:
	uv run pytest -q

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage