{{ config(unique_key='paid_date, store_id, method', order_by='(paid_date, store_id, method)') }}

-- Money movements per day the payment happened (Vietnam time), store and method.
-- Refunds are negative payment rows, so net is simply the sum.

with payments as (
    select
        p.order_id                              as order_id,
        p.kind                                  as kind,
        p.method                                as method,
        p.amount_vnd                            as amount_vnd,
        toDate(p.paid_at, 'Asia/Ho_Chi_Minh')   as paid_date
    from {{ ref('silver_payments') }} as p
    {% if is_incremental() %}
    where toDate(p.paid_at, 'Asia/Ho_Chi_Minh') in (
        select distinct toDate(paid_at, 'Asia/Ho_Chi_Minh')
        from {{ ref('silver_payments') }}
        where extracted_on = toDate('{{ var("dt") }}')
    )
    {% endif %}
)

select
    p.paid_date                                      as paid_date,
    o.store_id                                       as store_id,
    p.method                                         as method,
    toInt64(countIf(p.kind = 'charge'))              as charges,
    toInt64(sumIf(p.amount_vnd, p.kind = 'charge'))  as charged_vnd,
    toInt64(-sumIf(p.amount_vnd, p.kind = 'refund')) as refunded_vnd,
    toInt64(sum(p.amount_vnd))                       as net_vnd
from payments as p
inner join {{ ref('silver_orders') }} as o on p.order_id = o.order_id
group by paid_date, store_id, method
