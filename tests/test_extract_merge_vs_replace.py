"""Test merge vs replace write dispositions for idempotency."""

from unittest.mock import MagicMock

import duckdb
import pytest

from pipeline.extract import run_dlt


def get_db_path() -> str:
    """Get the path to the DuckDB database created by DLT."""
    import os

    # Use the environment variable set by mock_env
    return os.getenv("DUCKDB_PATH", "feefo_pipeline.duckdb")


@pytest.mark.parametrize(
    "mode,expected_behavior",
    [
        ("merge", "idempotent"),  # Running twice should yield same count
        ("replace", "idempotent"),  # Running twice should yield same count (replaces)
        ("append", "additive"),  # Running twice should double the count
    ],
)
def test_write_disposition_behavior(
    mock_env: None, mock_requests: MagicMock, mode: str, expected_behavior: str
) -> None:
    """
    Test that different write dispositions behave correctly on repeated runs.

    Args:
        mock_env: Environment setup fixture
        mock_requests: Mocked requests.get
        mode: Write mode (merge, replace, or append)
        expected_behavior: Expected behavior ("idempotent" or "additive")
    """
    # Run pipeline first time
    run_dlt(
        merchant_id="test-merchant",
        mode=mode,
        max_pages=1,
        include_ratings=False,
    )

    # Check data after first run
    db_path = get_db_path()
    conn = duckdb.connect(db_path)
    result = conn.execute("SELECT COUNT(*) FROM bronze.feefo_reviews").fetchone()
    first_count = result[0] if result else 0
    conn.close()

    # Run pipeline second time with same mode
    run_dlt(
        merchant_id="test-merchant",
        mode=mode,
        max_pages=1,
        include_ratings=False,
    )

    # Check data after second run
    conn = duckdb.connect(db_path)
    result = conn.execute("SELECT COUNT(*) FROM bronze.feefo_reviews").fetchone()
    second_count = result[0] if result else 0
    conn.close()

    # Verify behavior based on mode
    assert first_count > 0, f"First run in {mode} mode should insert data"

    if expected_behavior == "idempotent":
        assert (
            second_count == first_count
        ), f"{mode.capitalize()} mode should be idempotent (same count after second run)"
    elif expected_behavior == "additive":
        assert second_count == first_count * 2, f"{mode.capitalize()} mode should double the data on second run"
