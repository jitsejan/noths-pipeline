{{
    config(
        materialized='view'
    )
}}

-- Summary Output 2: Top 5 products by number of reviews
-- Identifies the most reviewed products

select
    ps.merchant_id,
    ps.product_sku,
    ps.product_title,
    ps.review_count,
    ps.avg_product_review_rating,
    ps.catalog_rating,
    ps.overall_sentiment,
    ps.positive_reviews,
    ps.negative_reviews,
    ps.latest_review_date

from {{ ref('product_summary') }} as ps
order by 4 desc
limit 5
