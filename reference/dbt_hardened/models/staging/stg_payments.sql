select
    assumeNotNull(payment_id)  as payment_id,
    assumeNotNull(order_id)    as order_id,
    assumeNotNull(kind)        as kind,
    assumeNotNull(method)      as method,
    toInt64(amount_vnd)        as amount_vnd,
    assumeNotNull(paid_at)     as paid_at,
    _dt                        as extracted_on
from {{ source('bronze', 'bronze_payments') }}
