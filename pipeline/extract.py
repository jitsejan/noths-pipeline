"""Extract functions for Feefo API data ingestion."""

from typing import Any, Optional

import dlt
import requests
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.rest_api import rest_api_source

from pipeline.settings import (
    DEFAULT_MAX_PAGES,
    DEFAULT_MERCHANT_ID,
    FEEFO_API_BASE_URL,
)


@dlt.resource(name="feefo_products_for_reviews", write_disposition="merge", primary_key="sku")
def fetch_products_for_skus(merchant_id: str, skus: list[str]) -> Any:
    """
    Fetch product ratings for specific SKUs.

    Args:
        merchant_id: Merchant identifier
        skus: List of product SKUs to fetch

    Yields:
        Product rating data for each SKU
    """
    for sku in skus:
        url = f"{FEEFO_API_BASE_URL}/products/ratings"
        params = {
            "merchant_identifier": merchant_id,
            "product_sku": sku,
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        # The API returns products array even for single SKU queries
        if "products" in data and data["products"]:
            yield from data["products"]


def feefo_reviews(
    merchant_id: str = DEFAULT_MERCHANT_ID,
    max_pages: int = DEFAULT_MAX_PAGES,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> Any:
    """
    Create a DLT source for Feefo reviews and products.

    Args:
        merchant_id: Merchant identifier (e.g., 'notonthehighstreet-com')
        max_pages: Maximum number of pages to fetch
        since: Optional start date filter
        until: Optional end date filter

    Returns:
        DLT source for Feefo API data
    """
    # Build query parameters
    params = {"merchant_identifier": merchant_id}
    if since:
        params["since"] = since
    if until:
        params["until"] = until

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

    return rest_api_source(config)


def run_dlt(
    merchant_id: str = DEFAULT_MERCHANT_ID,
    mode: str = "merge",
    max_pages: int = DEFAULT_MAX_PAGES,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> None:
    """
    Run DLT pipeline to load Feefo data into DuckDB.

    Args:
        merchant_id: Merchant identifier (e.g., 'notonthehighstreet-com')
        mode: Write mode - 'merge', 'replace', or 'append'
        max_pages: Maximum number of pages to fetch
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

    # Step 1: Get reviews source
    source = feefo_reviews(merchant_id=merchant_id, max_pages=max_pages, since=since, until=until)

    # Apply write disposition to resources
    for resource in source.resources.values():
        resource.apply_hints(write_disposition=write_disposition)

    # Step 2: Run pipeline to load reviews
    print("Loading reviews...")  # noqa: T201
    load_info = pipeline.run(source)
    print(load_info)  # noqa: T201

    # Step 3: Extract SKUs from reviews and fetch product ratings
    print("\nExtracting SKUs from reviews...")  # noqa: T201

    # Query the loaded reviews to get unique product SKUs
    import duckdb
    conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

    try:
        skus_result = conn.execute("""
            SELECT DISTINCT product__sku
            FROM bronze.feefo_reviews__products
            WHERE product__sku IS NOT NULL
        """).fetchall()

        skus = [row[0] for row in skus_result]
        print(f"Found {len(skus)} unique product SKUs in reviews")  # noqa: T201

        if skus:
            # Step 4: Fetch product ratings for those SKUs
            print("Fetching product ratings for reviewed SKUs...")  # noqa: T201
            products_resource = fetch_products_for_skus(merchant_id, skus)
            products_resource.apply_hints(write_disposition=write_disposition)

            load_info = pipeline.run(products_resource)
            print(load_info)  # noqa: T201
        else:
            print("No product SKUs found in reviews")  # noqa: T201
    finally:
        conn.close()
