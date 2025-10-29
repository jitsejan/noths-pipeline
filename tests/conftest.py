"""Shared test fixtures and configuration."""

import os
from pathlib import Path
from typing import Any, Generator
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def tmp_duckdb_path(tmp_path: Path) -> Generator[str, None, None]:
    """
    Provide a temporary DuckDB database path.

    Args:
        tmp_path: pytest's built-in tmp_path fixture

    Yields:
        Path to temporary DuckDB file
    """
    db_path = tmp_path / "test_feefo.duckdb"
    yield str(db_path)
    # Cleanup happens automatically with tmp_path


@pytest.fixture
def mock_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Generator[None, None, None]:
    """
    Set up environment variables for testing and clean up database after test.

    Args:
        monkeypatch: pytest's monkeypatch fixture
        tmp_path: pytest's built-in tmp_path fixture

    Yields:
        None
    """
    # Set DLT pipeline directory to temp location
    dlt_dir = tmp_path / ".dlt"
    monkeypatch.setenv("DLT_PROJECT_DIR", str(dlt_dir))
    monkeypatch.setenv("DLT_DATA_DIR", str(dlt_dir / "data"))
    monkeypatch.setenv("DLT_PIPELINE_DIR", str(dlt_dir / "pipelines"))

    # Set test database path to temp directory (isolate from production data!)
    test_db_path = tmp_path / "test_feefo_pipeline.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(test_db_path))

    yield

    # Cleanup happens automatically with tmp_path


@pytest.fixture
def mock_reviews_response() -> dict[str, Any]:
    """
    Mock Feefo reviews API response factory that returns different data per page.

    Returns:
        Function that generates sample reviews API response for a given page
    """
    def get_response_for_page(page: int) -> dict[str, Any]:
        """Generate response for a specific page."""
        return {
            "reviews": [
                {
                    "url": f"https://feefo.com/review/{page}-1",
                    "id": f"review-{page}-1",
                    "merchant": {
                        "identifier": "test-merchant"
                    },
                    "customer": {
                        "display_name": f"Test Customer {page}-1"
                    },
                    "service": {
                        "rating": {
                            "rating": 5
                        }
                    },
                    "products": [
                        {
                            "product": {
                                "sku": f"SKU-{page:03d}-1",
                                "title": f"Test Product {page}-1"
                            },
                            "rating": {
                                "rating": 5
                            }
                        }
                    ]
                },
                {
                    "url": f"https://feefo.com/review/{page}-2",
                    "id": f"review-{page}-2",
                    "merchant": {
                        "identifier": "test-merchant"
                    },
                    "customer": {
                        "display_name": f"Test Customer {page}-2"
                    },
                    "service": {
                        "rating": {
                            "rating": 4
                        }
                    },
                    "products": [
                        {
                            "product": {
                                "sku": f"SKU-{page:03d}-2",
                                "title": f"Test Product {page}-2"
                            },
                            "rating": {
                                "rating": 4
                            }
                        }
                    ]
                }
            ],
            "summary": {
                "meta": {
                    "pages": 3,
                    "page": page,
                    "count": 2
                }
            }
        }

    return get_response_for_page


@pytest.fixture
def mock_product_ratings_response() -> dict[str, Any]:
    """
    Mock Feefo product ratings API response.

    Returns:
        Sample product ratings API response
    """
    return {
        "products": [
            {
                "sku": "SKU-001",
                "title": "Test Product 1",
                "rating": {
                    "rating": 4.5,
                    "count": 10
                }
            }
        ]
    }


@pytest.fixture
def mock_requests(monkeypatch: pytest.MonkeyPatch, mock_reviews_response: Any, mock_product_ratings_response: dict[str, Any]) -> MagicMock:
    """
    Mock HTTP requests at multiple levels to intercept DLT and direct requests calls.

    Args:
        monkeypatch: pytest's monkeypatch fixture
        mock_reviews_response: Mock reviews response factory function
        mock_product_ratings_response: Mock product ratings response

    Returns:
        Mock requests object for call tracking
    """
    import requests
    from requests import Response

    call_tracker = MagicMock()

    def create_mock_response(url: str, params: dict[str, Any] | None = None) -> Response:
        """Create a proper Response object with mocked data."""
        response = Response()
        response.status_code = 200
        response._content = b''

        # Return different responses based on URL
        if "reviews/all" in url:
            # Check page parameter to support pagination
            page = params.get("page", 1) if params else 1
            # Convert page to int if it's a string
            if isinstance(page, str):
                page = int(page)
            response_data = mock_reviews_response(page)
            import json
            response._content = json.dumps(response_data).encode('utf-8')
        elif "products/ratings" in url:
            import json
            response._content = json.dumps(mock_product_ratings_response).encode('utf-8')
        else:
            response._content = b'{}'

        response.headers['Content-Type'] = 'application/json'
        return response

    def mock_get(url: str, *args: Any, **kwargs: Any) -> Response:
        """Mock requests.get with tracking."""
        params = kwargs.get("params", {})
        call_tracker(url, *args, **kwargs)
        return create_mock_response(url, params)

    def mock_session_send(self: Any, request: Any, **kwargs: Any) -> Response:
        """Mock Session.send method."""
        # Extract params from the prepared request URL
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(request.url)
        params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}

        call_tracker(request.url, params=params, **kwargs)
        return create_mock_response(request.url, params)

    # Patch both requests.get and Session.send to cover all cases
    monkeypatch.setattr("requests.get", mock_get)
    monkeypatch.setattr("requests.Session.send", mock_session_send)

    return call_tracker
