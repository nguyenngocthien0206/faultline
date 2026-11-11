{{ config(partition_by='order_date', order_by='(order_date, store_id)') }}

-- Revenue per business day (the day the order was placed, in Vietnam time).
--
-- Hardened: counts final states instead of netting change events. An order
-- counts if its latest status is completed; a refunded or voided one counts
-- zero, however many of its change rows were extracted, and in whatever order
-- they arrived. Every order of the day is grouped (not only completed ones), so
-- a store-day whose orders were all refunded still gets its row of zeros, and
-- insert_overwrite replaces the old figures instead of leaving them behind.
--
-- Incremental: rewrites the business days touched by this run's extract (the
-- orders and order lines that came in, wherever their day lies) and swaps
-- each day in whole.

with touched_days as (
    select toDate(ordered_at, 'Asia/Ho_Chi_Minh') as order_date
    from {{ ref('stg_orders') }}
    where extracted_on = toDate('{{ var("dt") }}')

    union distinct

    select toDate(o.ordered_at, 'Asia/Ho_Chi_Minh') as order_date
    from {{ ref('stg_order_items') }} as i
    inner join {{ ref('silver_orders') }} as o on i.order_id = o.order_id
    where i.extracted_on = toDate('{{ var("dt") }}')
),

day_orders as (
    select
        order_id,
        store_id,
        toDate(ordered_at, 'Asia/Ho_Chi_Minh') as order_date,
        status = 'completed'                   as counts,
        subtotal_vnd,
        discount_vnd,
        total_vnd
    from {{ ref('silver_orders') }}
    {% if is_incremental() %}
    where toDate(ordered_at, 'Asia/Ho_Chi_Minh') in (select order_date from touched_days)
    {% endif %}
),

lines as (
    select order_id, sum(qty) as qty
    from {{ ref('silver_order_items') }}
    group by order_id
)

select
    o.order_date                                as order_date,
    o.store_id                                  as store_id,
    toInt64(countIf(o.counts))                  as orders,
    toInt64(sumIf(l.qty, o.counts))             as items_sold,
    toInt64(sumIf(o.subtotal_vnd, o.counts))    as gross_vnd,
    toInt64(sumIf(o.discount_vnd, o.counts))    as discount_vnd,
    toInt64(sumIf(o.total_vnd, o.counts))       as net_vnd
from day_orders as o
left join lines as l on o.order_id = l.order_id
group by order_date, store_id
