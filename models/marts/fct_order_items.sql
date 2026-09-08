{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_item_id',
    on_schema_change='append_new_columns',
    cluster_by=['order_date']
) }}

{% if is_incremental() %}
with previous_fact_state as (
    select
        order_item_id,
        order_date,
        previous_order_date
    from {{ this }}
),
{% else %}
with
{% endif %}
order_items as (
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
        {% if is_incremental() %}
        case
            when previous.order_date <> oi.order_date then previous.order_date
            else previous.previous_order_date
        end as previous_order_date,
        {% else %}
        cast(null as date) as previous_order_date,
        {% endif %}
        o.status,
        o.payment_method,
        oi.currency,
        p.category,
        c.segment,
        oi.quantity,
        oi.unit_price,
        cast(oi.quantity * oi.unit_price as decimal(20, 2)) as line_revenue,
        greatest(oi.updated_at, o.updated_at, p.updated_at, c.updated_at) as source_updated_at,
        greatest(oi._ingested_at, o._ingested_at, p._ingested_at, c._ingested_at)
            as pipeline_ingested_at
    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    inner join products p on oi.product_id = p.product_id
    inner join customers c on o.customer_id = c.customer_id
    {% if is_incremental() %}
    left join previous_fact_state previous on oi.order_item_id = previous.order_item_id
    {% endif %}
)
select * from joined
{% if is_incremental() %}
where pipeline_ingested_at >= (
    select coalesce(max(pipeline_ingested_at), cast('1900-01-01' as timestamp)) from {{ this }}
)
{% endif %}
