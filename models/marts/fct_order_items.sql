{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_item_id',
    cluster_by=['order_date']
) }}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
products as (
    select * from {{ ref('stg_products') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
joined as (
    select
        oi.order_item_id,
        oi.order_id,
        o.customer_id,
        oi.product_id,
        oi.order_date,
        o.status,
        o.payment_method,
        oi.currency,
        p.category,
        c.segment,
        oi.quantity,
        oi.unit_price,
        cast(oi.quantity * oi.unit_price as decimal(20, 2)) as line_revenue,
        greatest(oi.updated_at, o.updated_at, p.updated_at, c.updated_at) as source_updated_at
    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    inner join products p on oi.product_id = p.product_id
    inner join customers c on o.customer_id = c.customer_id
)
select * from joined
{% if is_incremental() %}
where source_updated_at >= (
    select coalesce(max(source_updated_at), cast('1900-01-01' as timestamp)) from {{ this }}
)
{% endif %}
