-- One row per order line. The lag window re-extracts recent lines on every
-- run and a rerun extracts them again; all those copies collapse here.
--
-- `extracted_on` is deliberately left out: when the platform happened to read
-- a line is lineage, not a fact about the sale, and it differs between a clean
-- run and one where the line arrived late. It stays in stg_order_items.
with copies as (
    select *, created_at as version
    from {{ ref('stg_order_items') }}
)

select
    order_id,
    line_no,
    argMax(item_id, version)         as item_id,
    argMax(qty, version)             as qty,
    argMax(unit_price_vnd, version)  as unit_price_vnd,
    max(version)                     as created_at
from copies
group by order_id, line_no
