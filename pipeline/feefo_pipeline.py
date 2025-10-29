"""Feefo API pipeline using DLT rest_api source."""

import logging

from pipeline.extract import run_dlt

# Configure logging
logger = logging.getLogger(__name__)


def load_feefo() -> None:
    """
    Load Feefo reviews and products data into DuckDB.

    This is a convenience wrapper around run_dlt with default parameters.
    """
    logger.info("Starting Feefo data load")
    try:
        run_dlt(mode="merge", max_pages=1)
        logger.info("Feefo data load completed successfully")
    except Exception as e:
        logger.error("Feefo data load failed: %s", e)
        raise


if __name__ == "__main__":
    load_feefo()
