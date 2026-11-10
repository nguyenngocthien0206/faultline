select
    assumeNotNull(order_id)    as order_id,
    toInt32(line_no)           as line_no,
    assumeNotNull(item_id)     as item_id,
    toInt32(qty)               as qty,
    toInt64(unit_price_vnd)    as unit_price_vnd,
    assumeNotNull(created_at)  as created_at,
    _dt                        as extracted_on
from {{ source('bronze', 'bronze_order_items') }}
