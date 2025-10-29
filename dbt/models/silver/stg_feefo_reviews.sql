{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('bronze', 'feefo_reviews') }}
),

renamed as (
    select
        url as review_id,
        COALESCE(merchant__identifier, 'unknown') as merchant_id,
        service__id as service_id,
        service__rating__rating as service_rating,
        COALESCE(service__rating__min, 1) as rating_min,
        COALESCE(service__rating__max, 5) as rating_max,
        COALESCE(service__moderation_status, 'unmoderated') as moderation_status,
        COALESCE(service__feedback_verification_state, 'unverified') as verification_state,
        service__created_at as created_at,
        COALESCE(service__helpful_votes, 0) as helpful_votes,
        COALESCE(locale, 'en_GB') as locale,
        last_updated_date,
        _dlt_load_id,
        _dlt_id

    from source
    where COALESCE(merchant__identifier, 'unknown') = '{{ var("merchant_id") }}'
)

select * from renamed
