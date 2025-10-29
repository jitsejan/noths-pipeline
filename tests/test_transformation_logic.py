"""Test transformation logic for SKU extraction and product enrichment."""

from unittest.mock import MagicMock, patch
from typing import Any

import pytest

from pipeline.extract import fetch_products_from_reviews


def test_sku_extraction_from_nested_reviews() -> None:
    """
    Test that SKUs are correctly extracted from nested review structure.

    Verifies the transformer can navigate: review → products → product → sku
    """
    # Mock reviews with nested structure
    mock_reviews = [
        {
            "url": "https://feefo.com/review/1",
            "products": [
                {
                    "product": {
                        "sku": "TEST-SKU-001",
                        "title": "Test Product 1"
                    }
                }
            ]
        }
    ]

    mock_product_response = {
        "products": [
            {
                "sku": "TEST-SKU-001",
                "rating": {"rating": 4.5, "count": 10}
            }
        ]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_product_response
        mock_get.return_value.raise_for_status = MagicMock()

        # Transform the reviews
        result = list(fetch_products_from_reviews(
            merchant_id="test-merchant",
            reviews_resource=iter(mock_reviews),
            period_days=None
        ))

        # Should have called API with correct SKU
        assert mock_get.call_count == 1
        call_args = mock_get.call_args
        assert call_args.kwargs["params"]["product_sku"] == "TEST-SKU-001"

        # Should yield the product data
        assert len(result) == 1
        assert result[0]["sku"] == "TEST-SKU-001"


def test_sku_deduplication() -> None:
    """
    Test that duplicate SKUs are only fetched once.

    This verifies the seen_skus set prevents redundant API calls.
    """
    # Mock reviews with duplicate SKUs
    mock_reviews = [
        {
            "url": "https://feefo.com/review/1",
            "products": [
                {"product": {"sku": "DUPLICATE-SKU", "title": "Product A"}}
            ]
        },
        {
            "url": "https://feefo.com/review/2",
            "products": [
                {"product": {"sku": "DUPLICATE-SKU", "title": "Product A"}}  # Same SKU
            ]
        },
        {
            "url": "https://feefo.com/review/3",
            "products": [
                {"product": {"sku": "UNIQUE-SKU", "title": "Product B"}}
            ]
        }
    ]

    mock_product_response = {
        "products": [
            {"sku": "TEST", "rating": {"rating": 4.5}}
        ]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_product_response
        mock_get.return_value.raise_for_status = MagicMock()

        # Transform the reviews
        list(fetch_products_from_reviews(
            merchant_id="test-merchant",
            reviews_resource=iter(mock_reviews),
            period_days=None
        ))

        # Should only call API twice (once for DUPLICATE-SKU, once for UNIQUE-SKU)
        assert mock_get.call_count == 2

        # Verify the unique SKUs that were called
        called_skus = [
            call.kwargs["params"]["product_sku"]
            for call in mock_get.call_args_list
        ]
        assert set(called_skus) == {"DUPLICATE-SKU", "UNIQUE-SKU"}


def test_multiple_products_in_single_review() -> None:
    """
    Test that multiple products in a single review are all processed.
    """
    # Mock review with multiple products
    mock_reviews = [
        {
            "url": "https://feefo.com/review/1",
            "products": [
                {"product": {"sku": "SKU-A", "title": "Product A"}},
                {"product": {"sku": "SKU-B", "title": "Product B"}},
                {"product": {"sku": "SKU-C", "title": "Product C"}}
            ]
        }
    ]

    mock_product_response = {
        "products": [{"sku": "TEST", "rating": {"rating": 5.0}}]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_product_response
        mock_get.return_value.raise_for_status = MagicMock()

        # Transform the reviews
        list(fetch_products_from_reviews(
            merchant_id="test-merchant",
            reviews_resource=iter(mock_reviews),
            period_days=None
        ))

        # Should call API 3 times (once per unique SKU)
        assert mock_get.call_count == 3

        # Verify all SKUs were fetched
        called_skus = [
            call.kwargs["params"]["product_sku"]
            for call in mock_get.call_args_list
        ]
        assert set(called_skus) == {"SKU-A", "SKU-B", "SKU-C"}


def test_period_days_parameter_propagation() -> None:
    """
    Test that period_days parameter is correctly added to product API calls.
    """
    mock_reviews = [
        {
            "url": "https://feefo.com/review/1",
            "products": [{"product": {"sku": "TEST-SKU"}}]
        }
    ]

    mock_product_response = {
        "products": [{"sku": "TEST-SKU", "rating": {"rating": 4.0}}]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_product_response
        mock_get.return_value.raise_for_status = MagicMock()

        # Transform with period_days=30
        list(fetch_products_from_reviews(
            merchant_id="test-merchant",
            reviews_resource=iter(mock_reviews),
            period_days=30
        ))

        # Verify period was added to params
        call_params = mock_get.call_args.kwargs["params"]
        assert "since_period" in call_params
        assert call_params["since_period"] == "30days"


def test_no_period_parameter_when_none() -> None:
    """
    Test that period parameter is omitted when period_days=None.
    """
    mock_reviews = [
        {
            "url": "https://feefo.com/review/1",
            "products": [{"product": {"sku": "TEST-SKU"}}]
        }
    ]

    mock_product_response = {
        "products": [{"sku": "TEST-SKU", "rating": {"rating": 4.0}}]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_product_response
        mock_get.return_value.raise_for_status = MagicMock()

        # Transform with period_days=None (default)
        list(fetch_products_from_reviews(
            merchant_id="test-merchant",
            reviews_resource=iter(mock_reviews),
            period_days=None
        ))

        # Verify period was NOT added to params
        call_params = mock_get.call_args.kwargs["params"]
        assert "since_period" not in call_params


def test_missing_sku_is_skipped() -> None:
    """
    Test that reviews with missing SKUs are gracefully skipped.
    """
    # Mock reviews with missing/None SKU
    mock_reviews = [
        {
            "url": "https://feefo.com/review/1",
            "products": [
                {"product": {"sku": None, "title": "Product without SKU"}}
            ]
        },
        {
            "url": "https://feefo.com/review/2",
            "products": [
                {"product": {"title": "Product missing SKU field"}}  # No sku key
            ]
        },
        {
            "url": "https://feefo.com/review/3",
            "products": [
                {"product": {"sku": "VALID-SKU", "title": "Valid Product"}}
            ]
        }
    ]

    mock_product_response = {
        "products": [{"sku": "VALID-SKU", "rating": {"rating": 5.0}}]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_product_response
        mock_get.return_value.raise_for_status = MagicMock()

        # Transform the reviews
        list(fetch_products_from_reviews(
            merchant_id="test-merchant",
            reviews_resource=iter(mock_reviews),
            period_days=None
        ))

        # Should only call API once (for VALID-SKU)
        assert mock_get.call_count == 1
        call_params = mock_get.call_args.kwargs["params"]
        assert call_params["product_sku"] == "VALID-SKU"


def test_empty_products_array_is_handled() -> None:
    """
    Test that reviews with empty products array don't cause errors.
    """
    mock_reviews = [
        {
            "url": "https://feefo.com/review/1",
            "products": []  # Empty products
        },
        {
            "url": "https://feefo.com/review/2",
            "products": [
                {"product": {"sku": "VALID-SKU"}}
            ]
        }
    ]

    mock_product_response = {
        "products": [{"sku": "VALID-SKU", "rating": {"rating": 4.5}}]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_product_response
        mock_get.return_value.raise_for_status = MagicMock()

        # Should not raise an error
        result = list(fetch_products_from_reviews(
            merchant_id="test-merchant",
            reviews_resource=iter(mock_reviews),
            period_days=None
        ))

        # Should only process the valid SKU
        assert mock_get.call_count == 1


@pytest.mark.parametrize("period_days,expected_param", [
    (7, "7days"),
    (30, "30days"),
    (90, "90days"),
    (365, "365days"),
])
def test_period_days_formatting(period_days: int, expected_param: str) -> None:
    """
    Test that various period_days values are correctly formatted.

    Args:
        period_days: Number of days for the period
        expected_param: Expected formatted string parameter
    """
    mock_reviews = [
        {
            "url": "https://feefo.com/review/1",
            "products": [{"product": {"sku": "TEST-SKU"}}]
        }
    ]

    mock_product_response = {
        "products": [{"sku": "TEST-SKU", "rating": {"rating": 4.0}}]
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_product_response
        mock_get.return_value.raise_for_status = MagicMock()

        # Transform with specific period_days
        list(fetch_products_from_reviews(
            merchant_id="test-merchant",
            reviews_resource=iter(mock_reviews),
            period_days=period_days
        ))

        # Verify correct formatting
        call_params = mock_get.call_args.kwargs["params"]
        assert call_params["since_period"] == expected_param
