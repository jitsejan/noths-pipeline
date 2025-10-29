-- Test that product_rating is within valid range (1.0 to 5.0)
-- This test should return 0 rows (all ratings are within range)

select
    product_sku,
    product_rating
from {{ ref('stg_feefo_product_ratings') }}
where product_rating is not null
  and (product_rating < 1.0 or product_rating > 5.0)
