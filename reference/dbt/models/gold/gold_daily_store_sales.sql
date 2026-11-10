{{ config(unique_key='order_date, store_id', order_by='(order_date, store_id)') }}

-- Revenue per business day (the day the order was placed, in Vietnam time).
--
-- Naive: nets change events instead of reading final states. A completed row
-- adds the order, a refunded row subtracts it, a voided row counts zero. On the
-- happy path every event arrives exactly once and the numbers are exact; any
-- duplicated event is counted again, silently.
--
-- Incremental: only the business days touched by this run's extract are
-- recomputed (new orders, plus the original days of refunds that came in).

with events as (
    select
        order_id,
        store_id,
        toDate(ordered_at, 'Asia/Ho_Chi_Minh')                     as order_date,
        multiIf(status = 'completed', 1, status = 'refunded', -1, 0) as sign,
        subtotal_vnd,
        discount_vnd,
        total_vnd
    from {{ ref('silver_order_events') }}
    {% if is_incremental() %}
    where toDate(ordered_at, 'Asia/Ho_Chi_Minh') in (
        select distinct toDate(ordered_at, 'Asia/Ho_Chi_Minh')
        from {{ ref('silver_order_events') }}
        where extracted_on = toDate('{{ var("dt") }}')
    )
    {% endif %}
),

lines as (
    select order_id, sum(qty) as qty
    from {{ ref('silver_order_items') }}
    group by order_id
)

select
    e.order_date                                    as order_date,
    e.store_id                                      as store_id,
    toInt64(sum(e.sign))                            as orders,
    toInt64(sum(e.sign * l.qty))                    as items_sold,
    toInt64(sum(e.sign * e.subtotal_vnd))           as gross_vnd,
    toInt64(sum(e.sign * e.discount_vnd))           as discount_vnd,
    toInt64(sum(e.sign * e.total_vnd))              as net_vnd
from events as e
left join lines as l on e.order_id = l.order_id
group by order_date, store_id
