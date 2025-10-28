"""Feefo API pipeline using DLT rest_api source."""

from pipeline.extract import run_dlt


def load_feefo() -> None:
    """Load Feefo reviews and products data into DuckDB."""
    run_dlt(mode="merge", max_pages=1)


if __name__ == "__main__":
    load_feefo()
