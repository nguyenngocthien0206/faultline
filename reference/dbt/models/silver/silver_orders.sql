-- Latest state of each order. Grouping by order_id makes this table look
-- perfectly clean (one row per key) even when the same change arrived twice:
-- a uniqueness check on silver passes while gold is wrong.
--
-- `version` exists because ClickHouse resolves aliases across the whole SELECT:
-- `max(updated_at) as updated_at` would turn every `argMax(x, updated_at)` into
-- an aggregate nested in an aggregate.
with events as (
    select *, updated_at as version
    from {{ ref('silver_order_events') }}
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
from events
group by order_id
