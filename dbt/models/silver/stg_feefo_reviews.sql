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
        url review_id,
        service__id service_id,
        service__rating__rating service_rating,
        service__created_at created_at,
        last_updated_date,
        _dlt_load_id,
        _dlt_id,
        coalesce(merchant__identifier, 'unknown') merchant_id,
        coalesce(service__rating__min, 1) rating_min,
        coalesce(service__rating__max, 5) rating_max,
        coalesce(service__moderation_status, 'unmoderated') moderation_status,
        coalesce(service__feedback_verification_state, 'unverified')
            verification_state,
        coalesce(service__helpful_votes, 0) helpful_votes,
        coalesce(locale, 'en_GB') locale

    from source
    where coalesce(merchant__identifier, 'unknown') = '{{ var("merchant_id") }}'
)

select * from renamed
