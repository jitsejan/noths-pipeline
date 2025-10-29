-- Test that critical fields have been properly handled for nulls
-- After COALESCE transformations, these fields should never be null
-- This test should return 0 rows

select
    review_id,
    merchant_id,
    rating_min,
    rating_max,
    moderation_status,
    verification_state,
    helpful_votes,
    review_locale
from {{ ref('stg_feefo_reviews') }}
where merchant_id is null
   or rating_min is null
   or rating_max is null
   or moderation_status is null
   or verification_state is null
   or helpful_votes is null
   or review_locale is null
