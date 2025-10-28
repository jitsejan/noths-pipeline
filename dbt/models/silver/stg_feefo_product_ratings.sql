{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('bronze', 'feefo_products_for_reviews') }}
),

renamed as (
    select
        sku as product_sku,
        rating as product_rating,
        _dlt_load_id,
        _dlt_id,
        '{{ var("merchant_id") }}' as merchant_id

    from source
)

select * from renamed
