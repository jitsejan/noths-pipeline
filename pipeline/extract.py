"""Extract functions for Feefo API data ingestion."""

from typing import Any, Optional

import dlt
import requests
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.rest_api import rest_api_source

from pipeline.settings import (
    DEFAULT_INCLUDE_RATINGS,
    DEFAULT_MAX_PAGES,
    DEFAULT_MERCHANT_ID,
    DEFAULT_PERIOD_DAYS,
    FEEFO_API_BASE_URL,
)


@dlt.resource(name="feefo_products_for_reviews", write_disposition="merge", primary_key="sku")
def fetch_products_from_reviews(
    merchant_id: str, reviews_resource: Any, period_days: Optional[int] = None
) -> Any:
    """
    Transformer that extracts SKUs from reviews and fetches product ratings.

    Args:
        merchant_id: Merchant identifier
        reviews_resource: The reviews resource to transform
        period_days: Optional number of days to filter ratings (e.g., 30 for last 30 days)

    Yields:
        Product rating data for SKUs found in reviews
    """
    seen_skus = set()

    # Process reviews as they come through
    for review in reviews_resource:
        # Extract products from nested structure
        products = review.get("products", [])

        for product in products:
            # Get SKU from nested product structure
            product_data = product.get("product", {})
            sku = product_data.get("sku")

            # Only fetch each SKU once
            if sku and sku not in seen_skus:
                seen_skus.add(sku)

                url = f"{FEEFO_API_BASE_URL}/products/ratings"
                params = {
                    "merchant_identifier": merchant_id,
                    "product_sku": sku,
                }

                # Add period filter if specified
                if period_days:
                    params["since_period"] = f"{period_days}days"

                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                # Yield product ratings
                if "products" in data and data["products"]:
                    yield from data["products"]


@dlt.source
def feefo_source(
    merchant_id: str = DEFAULT_MERCHANT_ID,
    max_pages: int = DEFAULT_MAX_PAGES,
    include_ratings: bool = DEFAULT_INCLUDE_RATINGS,
    period_days: Optional[int] = DEFAULT_PERIOD_DAYS,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> Any:
    """
    Create a DLT source for Feefo reviews and products.

    Args:
        merchant_id: Merchant identifier (e.g., 'notonthehighstreet-com')
        max_pages: Maximum number of pages to fetch
        include_ratings: Whether to fetch product ratings (default: True)
        period_days: Filter ratings by days (e.g., 30 for last 30 days, None for all time)
        since: Optional start date filter
        until: Optional end date filter

    Returns:
        DLT source with reviews and optionally enriched product ratings
    """
    # Build query parameters
    params = {"merchant_identifier": merchant_id}
    if since:
        params["since"] = since
    if until:
        params["until"] = until

    # Configure reviews resource
    config = {
        "client": {
            "base_url": FEEFO_API_BASE_URL,
        },
        "resources": [
            {
                "name": "feefo_reviews",
                "primary_key": "url",
                "endpoint": {
                    "path": "reviews/all",
                    "params": params,
                    "data_selector": "reviews",
                    "paginator": PageNumberPaginator(
                        base_page=1,
                        page_param="page",
                        total_path="summary.meta.pages",
                        maximum_page=max_pages,
                    ),
                },
            },
        ],
    }

    # Get the reviews resource from the REST API source
    reviews_source = rest_api_source(config)
    reviews = reviews_source.feefo_reviews

    # Conditionally create products resource
    if include_ratings:
        products = fetch_products_from_reviews(merchant_id, reviews, period_days)
        return reviews, products
    else:
        return (reviews,)


def run_dlt(
    merchant_id: str = DEFAULT_MERCHANT_ID,
    mode: str = "merge",
    max_pages: int = DEFAULT_MAX_PAGES,
    include_ratings: bool = DEFAULT_INCLUDE_RATINGS,
    period_days: Optional[int] = DEFAULT_PERIOD_DAYS,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> None:
    """
    Run DLT pipeline to load Feefo data into DuckDB.

    Args:
        merchant_id: Merchant identifier (e.g., 'notonthehighstreet-com')
        mode: Write mode - 'merge', 'replace', or 'append'
        max_pages: Maximum number of pages to fetch
        include_ratings: Whether to fetch product ratings (default: True)
        period_days: Filter ratings by days (e.g., 30 for last 30 days, None for all time)
        since: Optional start date filter
        until: Optional end date filter
    """
    # Map mode to write_disposition
    write_disposition_map = {
        "merge": "merge",
        "replace": "replace",
        "append": "append",
    }

    if mode not in write_disposition_map:
        raise ValueError(f"Invalid mode: {mode}. Must be one of: merge, replace, append")

    write_disposition = write_disposition_map[mode]

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="feefo_pipeline",
        destination="duckdb",
        dataset_name="bronze",
    )

    # Get source with reviews and optionally products
    source = feefo_source(
        merchant_id=merchant_id,
        max_pages=max_pages,
        include_ratings=include_ratings,
        period_days=period_days,
        since=since,
        until=until,
    )

    # Apply write disposition to all resources
    for resource in source.resources.values():
        resource.apply_hints(write_disposition=write_disposition)

    # Run pipeline
    if include_ratings:
        print("Loading Feefo reviews and product ratings...")  # noqa: T201
    else:
        print("Loading Feefo reviews (skipping product ratings)...")  # noqa: T201

    load_info = pipeline.run(source)
    print(load_info)  # noqa: T201
