-- Latest state of each order, whatever number of copies bronze holds.
-- The same query as the naive silver_orders: naive already reads order state
-- this way here; its flaw is that gold does not.
--
-- `version` exists because ClickHouse resolves aliases across the whole SELECT
-- (see the naive model).
with copies as (
    select *, updated_at as version
    from {{ ref('stg_orders') }}
)

select
    order_id,
    argMax(store_id, version)     as store_id,
    argMax(member_id, version)    as member_id,
    argMax(channel, version)      as channel,
    argMax(status, version)       as status,
    min(ordered_at)               as ordered_at,
    max(version)                  as updated_at,
    argMax(subtotal_vnd, version) as subtotal_vnd,
    argMax(discount_vnd, version) as discount_vnd,
    argMax(total_vnd, version)    as total_vnd,
    argMax(promo_code, version)   as promo_code,
    argMax(item_count, version)   as item_count
from copies
group by order_id
