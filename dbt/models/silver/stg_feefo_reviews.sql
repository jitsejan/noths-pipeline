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
        merchant__identifier as merchant_id,
        service__id as service_id,
        service__rating__rating as service_rating,
        service__rating__min as rating_min,
        service__rating__max as rating_max,
        service__moderation_status as moderation_status,
        service__feedback_verification_state as verification_state,
        service__created_at as created_at,
        service__helpful_votes as helpful_votes,
        locale,
        last_updated_date,
        _dlt_load_id,
        _dlt_id

    from source
    where merchant__identifier = '{{ var("merchant_id") }}'
)

select * from renamed
