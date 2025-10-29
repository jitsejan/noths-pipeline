"""Extract functions for Feefo API data ingestion."""

import logging
from collections.abc import Generator
from typing import Any

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

# Configure logging
logger = logging.getLogger(__name__)


@dlt.resource(name="feefo_products_for_reviews", write_disposition="merge", primary_key="sku")
def fetch_products_from_reviews(
    merchant_id: str, reviews_resource: Any, period_days: int | None = None
) -> Generator[dict[str, Any], None, None]:
    """
    Transformer that extracts SKUs from reviews and fetches product ratings.

    Args:
        merchant_id: Merchant identifier
        reviews_resource: The reviews resource to transform
        period_days: Optional number of days to filter ratings (e.g., 30 for last 30 days)

    Yields:
        Product rating data for SKUs found in reviews
    """
    seen_skus: set[str] = set()
    logger.info("Starting product rating enrichment for merchant: %s", merchant_id)

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
                logger.debug("Fetching ratings for SKU: %s", sku)

                url = f"{FEEFO_API_BASE_URL}/products/ratings"
                params = {
                    "merchant_identifier": merchant_id,
                    "product_sku": sku,
                }

                # Add period filter if specified
                if period_days:
                    params["since_period"] = f"{period_days}days"

                try:
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()

                    # Yield product ratings with sentiment analysis
                    if "products" in data and data["products"]:
                        for product in data["products"]:
                            # Add sentiment category based on rating and reviews
                            product["category"] = categorise_review(
                                rating=product.get("average_rating"),
                                review=product.get("review_text", ""),
                            )
                            yield product
                        logger.debug("Successfully fetched ratings for SKU: %s", sku)
                    else:
                        logger.warning("No product data found for SKU: %s", sku)

                except requests.exceptions.HTTPError as e:
                    logger.error("HTTP error fetching ratings for SKU %s: %s", sku, e)
                    # Continue processing other SKUs
                except requests.exceptions.RequestException as e:
                    logger.error("Request error fetching ratings for SKU %s: %s", sku, e)
                    # Continue processing other SKUs
                except ValueError as e:
                    logger.error("JSON decode error for SKU %s: %s", sku, e)
                    # Continue processing other SKUs

    logger.info("Completed product rating enrichment. Total unique SKUs processed: %d", len(seen_skus))


@dlt.source
def feefo_source(
    merchant_id: str = DEFAULT_MERCHANT_ID,
    max_pages: int = DEFAULT_MAX_PAGES,
    include_ratings: bool = DEFAULT_INCLUDE_RATINGS,
    period_days: int | None = DEFAULT_PERIOD_DAYS,
    since: str | None = None,
    until: str | None = None,
) -> tuple[Any, ...]:
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
        Tuple of DLT resources (reviews, and optionally products)
        Note: Return type uses Any due to DLT's dynamic resource system
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
    reviews_source = rest_api_source(config)  # type: ignore[arg-type]
    reviews = reviews_source.feefo_reviews

    # Conditionally create products resource (with sentiment analysis included)
    if include_ratings:
        products = fetch_products_from_reviews(merchant_id, reviews, period_days)
        return reviews, products
    else:
        return (reviews,)


def categorise_review(rating: float | None = None, review: str = "") -> str:
    """
    Simple sentiment analysis based on rating and keywords in the review text.

    Priority order:
    1. If rating is available, use it (4-5=positive, 3=neutral, 1-2=negative)
    2. Otherwise, scan review text for positive/negative keywords
    3. Default to neutral if no signals found

    Args:
        rating: Numerical rating (optional, 1-5 scale)
        review: Text of the review

    Returns:
        Sentiment category: 'positive', 'neutral', or 'negative'
    """
    # Define sentiment keywords
    positive_keywords = ("excellent", "amazing", "love", "perfect", "beautiful", "great", "fantastic")
    negative_keywords = ("disappointed", "poor", "terrible", "awful", "broken", "bad", "worst")

    # Priority 1: Use rating if available
    if rating is not None:
        if rating >= 4:
            return "positive"
        elif rating == 3:
            return "neutral"
        else:
            return "negative"

    # Priority 2: Scan review text for keywords
    review_lower = review.lower()
    review_words = review_lower.split()
    for word in review_words:
        if word in positive_keywords:
            return "positive"
        elif word in negative_keywords:
            return "negative"

    # Default: neutral
    return "neutral"


def run_dlt(
    merchant_id: str = DEFAULT_MERCHANT_ID,
    mode: str = "merge",
    max_pages: int = DEFAULT_MAX_PAGES,
    include_ratings: bool = DEFAULT_INCLUDE_RATINGS,
    period_days: int | None = DEFAULT_PERIOD_DAYS,
    since: str | None = None,
    until: str | None = None,
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

    Raises:
        ValueError: If mode is not one of 'merge', 'replace', or 'append'
        RuntimeError: If pipeline execution fails
    """
    logger.info("Starting DLT pipeline run")
    logger.info("Parameters: merchant_id=%s, mode=%s, max_pages=%d", merchant_id, mode, max_pages)
    logger.info("Options: include_ratings=%s, period_days=%s", include_ratings, period_days)

    # Map mode to write_disposition
    write_disposition_map = {
        "merge": "merge",
        "replace": "replace",
        "append": "append",
    }

    if mode not in write_disposition_map:
        error_msg = f"Invalid mode: {mode}. Must be one of: merge, replace, append"
        logger.error(error_msg)
        raise ValueError(error_msg)

    write_disposition = write_disposition_map[mode]

    try:
        # Create pipeline
        import os

        # Use environment variable for database path (for test isolation)
        db_path = os.getenv("DUCKDB_PATH", "data/feefo_pipeline.duckdb")
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
            logger.info("Database directory created/verified: %s", db_dir)

        logger.info("Creating DLT pipeline with database: %s", db_path)
        pipeline = dlt.pipeline(
            pipeline_name="feefo_pipeline",
            destination=dlt.destinations.duckdb(db_path),
            dataset_name="bronze",
        )

        # Get source with reviews and optionally products
        logger.info("Configuring data source")
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
            resource.apply_hints(write_disposition=write_disposition)  # type: ignore[arg-type]
            logger.debug("Applied write disposition '%s' to resource: %s", write_disposition, resource.name)

        # Run pipeline
        if include_ratings:
            logger.info("Loading Feefo reviews and product ratings...")
        else:
            logger.info("Loading Feefo reviews (skipping product ratings)...")

        load_info = pipeline.run(source)
        logger.info("Pipeline execution completed successfully")
        logger.info("Load info: %s", load_info)

    except ValueError:
        # Re-raise ValueError (already logged above)
        raise
    except Exception as e:
        error_msg = f"Pipeline execution failed: {e}"
        logger.exception(error_msg)
        raise RuntimeError(error_msg) from e
