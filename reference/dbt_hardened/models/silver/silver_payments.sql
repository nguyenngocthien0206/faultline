-- One row per payment (charges, and refunds as negative rows), however many
-- copies were extracted. Lineage stays in stg_payments, as for order lines.
with copies as (
    select *, paid_at as version
    from {{ ref('stg_payments') }}
)

select
    payment_id,
    argMax(order_id, version)    as order_id,
    argMax(kind, version)        as kind,
    argMax(method, version)      as method,
    argMax(amount_vnd, version)  as amount_vnd,
    max(version)                 as paid_at
from copies
group by payment_id
