{{
    config(
        materialized='view'
    )
}}

-- Summary Output 1: Average rating per product
-- Provides comprehensive rating information for all products

select
    ps.merchant_id,
    ps.product_sku,
    ps.product_title,
    ps.review_count,
    ps.avg_product_review_rating,
    ps.avg_service_rating,
    ps.catalog_rating,
    ps.rating_delta,
    ps.overall_sentiment,
    ps.positive_reviews,
    ps.negative_reviews,
    ps.latest_review_date,
    ps.first_review_date

from {{ ref('product_summary') }} as ps
order by 5 desc, 4 desc
