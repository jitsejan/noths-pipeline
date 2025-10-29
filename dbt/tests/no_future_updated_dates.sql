-- Test that last_updated_date is not in the future
-- This test should return 0 rows (all dates are in past or present)

select
    review_id,
    last_updated_date,
    current_date as today
from {{ ref('stg_feefo_reviews') }}
where CAST(last_updated_date AS DATE) > current_date
