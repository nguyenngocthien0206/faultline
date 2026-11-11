-- Every copy of every order row bronze holds: one per change at the source,
-- and again for each time the lag window or a rerun extracted it. Typed and
-- non-null; `extracted_on` is the landing partition the copy came in.
select
    assumeNotNull(order_id)              as order_id,
    assumeNotNull(store_id)              as store_id,
    member_id,
    assumeNotNull(channel)               as channel,
    assumeNotNull(status)                as status,
    assumeNotNull(ordered_at)            as ordered_at,
    assumeNotNull(updated_at)            as updated_at,
    toInt64(subtotal_vnd)                as subtotal_vnd,
    toInt64(discount_vnd)                as discount_vnd,
    toInt64(total_vnd)                   as total_vnd,
    promo_code,
    toInt32(item_count)                  as item_count,
    _dt                                  as extracted_on
from {{ source('bronze', 'bronze_orders') }}
