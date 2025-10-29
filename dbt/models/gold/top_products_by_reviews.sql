{{
    config(
        materialized='view'
    )
}}

-- Summary Output 2: Top 5 products by number of reviews
-- Identifies the most reviewed products

select
    merchant_id,
    product_sku,
    product_title,
    review_count,
    avg_product_review_rating,
    catalog_rating,
    overall_sentiment,
    positive_reviews,
    negative_reviews,
    latest_review_date

from {{ ref('product_summary') }}
order by review_count desc
limit 5
