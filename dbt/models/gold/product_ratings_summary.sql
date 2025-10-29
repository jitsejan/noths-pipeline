{{
    config(
        materialized='view'
    )
}}

-- Summary Output 1: Average rating per product
-- Provides comprehensive rating information for all products

select
    merchant_id,
    product_sku,
    product_title,
    review_count,
    avg_product_review_rating,
    avg_service_rating,
    catalog_rating,
    rating_delta,
    overall_sentiment,
    positive_reviews,
    negative_reviews,
    latest_review_date,
    first_review_date

from {{ ref('product_summary') }}
order by avg_product_review_rating desc, review_count desc
