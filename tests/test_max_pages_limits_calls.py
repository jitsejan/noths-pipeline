"""Test that max_pages parameter limits API pagination calls."""

from unittest.mock import MagicMock

import pytest

from pipeline.extract import run_dlt


def test_max_pages_limits_api_calls(mock_env: None, mock_requests: MagicMock) -> None:
    """
    Test that max_pages limits the number of API calls for pagination.

    Args:
        mock_env: Environment setup fixture
        mock_requests: Mocked requests.get call tracker
    """
    # Run with max_pages=1 (should only call reviews API once)
    run_dlt(
        merchant_id="test-merchant",
        mode="merge",
        max_pages=1,
        include_ratings=False,
    )

    # Count how many times the reviews API was called
    reviews_calls = [
        call for call in mock_requests.call_args_list
        if "reviews/all" in str(call[0][0])
    ]

    # Should only call reviews API once (page 1)
    assert len(reviews_calls) == 1, f"Expected 1 call to reviews API with max_pages=1, got {len(reviews_calls)}"


def test_max_pages_with_multiple_pages(mock_env: None, mock_requests: MagicMock) -> None:
    """
    Test that max_pages parameter is respected and limits pagination.

    Args:
        mock_env: Environment setup fixture
        mock_requests: Mocked requests.get call tracker
    """
    # Clear previous calls
    mock_requests.reset_mock()

    # Run with max_pages=2
    run_dlt(
        merchant_id="test-merchant",
        mode="merge",
        max_pages=2,
        include_ratings=False,
    )

    # Count how many times the reviews API was called
    reviews_calls = [
        call for call in mock_requests.call_args_list
        if "reviews/all" in str(call[0][0])
    ]

    # Should make at least 1 call and no more than max_pages (2) calls
    assert len(reviews_calls) >= 1, f"Expected at least 1 call to reviews API, got {len(reviews_calls)}"
    assert len(reviews_calls) <= 2, f"Expected no more than 2 calls to reviews API with max_pages=2, got {len(reviews_calls)}"

    # Verify that the page parameter was passed correctly in the first call
    first_call_url = str(reviews_calls[0][0][0])
    assert "page=1" in first_call_url, "First call should request page 1"


@pytest.mark.parametrize(
    "include_ratings,expected_product_calls",
    [
        (True, "at_least_one"),  # Should call product API
        (False, "none"),  # Should not call product API
    ],
)
def test_include_ratings_controls_product_api_calls(
    mock_env: None, mock_requests: MagicMock, include_ratings: bool, expected_product_calls: str
) -> None:
    """
    Test that include_ratings flag controls product ratings API calls.

    Args:
        mock_env: Environment setup fixture
        mock_requests: Mocked requests.get call tracker
        include_ratings: Whether to include product ratings
        expected_product_calls: Expected behavior ("at_least_one" or "none")
    """
    # Clear previous calls
    mock_requests.reset_mock()

    # Run with specified include_ratings flag
    run_dlt(
        merchant_id="test-merchant",
        mode="merge",
        max_pages=1,
        include_ratings=include_ratings,
    )

    # Count product API calls
    product_calls = [
        call for call in mock_requests.call_args_list
        if "products/ratings" in str(call[0][0])
    ]

    # Verify expected behavior
    if expected_product_calls == "at_least_one":
        assert len(product_calls) > 0, "Expected product ratings API calls when include_ratings=True"
    elif expected_product_calls == "none":
        assert len(product_calls) == 0, f"Expected no product ratings API calls when include_ratings=False, got {len(product_calls)}"
