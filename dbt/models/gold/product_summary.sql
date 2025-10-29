{{
    config(
        materialized='table'
    )
}}

with product_reviews as (
    -- Get individual product reviews from nested structure
    select
        rp.product__sku product_sku,
        rp.product__title product_title,
        rp.rating__rating product_review_rating,
        rp.review product_review_text,
        rp.created_at review_created_at,
        r.merchant_id,
        r.service_rating,
        r.service_id,
        rp._dlt_parent_id review_dlt_id
    from {{ source('bronze', 'feefo_reviews__products') }} as rp
    left join {{ ref('stg_feefo_reviews') }} as r
        on rp._dlt_parent_id = r._dlt_id
    where r.merchant_id = '{{ var("merchant_id") }}'
),

product_ratings as (
    -- Get overall product catalog ratings
    select
        product_sku,
        product_rating catalog_rating,
        merchant_id
    from {{ ref('stg_feefo_product_ratings') }}
),

aggregated as (
    select
        pr.merchant_id,
        pr.product_sku,
        max(pr.product_title) product_title,

        -- Review metrics
        count(distinct pr.service_id) review_count,
        round(avg(pr.product_review_rating), 2) avg_product_review_rating,
        round(avg(pr.service_rating), 2) avg_service_rating,

        -- Catalog rating
        max(prat.catalog_rating) catalog_rating,

        -- Rating comparison
        round(avg(pr.product_review_rating) - max(prat.catalog_rating), 2)
            rating_delta,

        -- Temporal
        max(pr.review_created_at) latest_review_date,
        min(pr.review_created_at) first_review_date,

        -- Text analytics
        count(case when pr.product_review_text is not null then 1 end)
            reviews_with_text,

        -- Sentiment flags
        sum(case when pr.product_review_rating <= 2 then 1 else 0 end)
            negative_reviews,
        sum(case when pr.product_review_rating >= 4 then 1 else 0 end)
            positive_reviews

    from product_reviews as pr
    left join product_ratings as prat
        on
            pr.product_sku = prat.product_sku
            and pr.merchant_id = prat.merchant_id
    group by pr.merchant_id, pr.product_sku
)

select
    *,
    case
        when negative_reviews > positive_reviews then 'negative'
        when positive_reviews > negative_reviews then 'positive'
        else 'neutral'
    end overall_sentiment
from aggregated
order by review_count desc, catalog_rating desc
