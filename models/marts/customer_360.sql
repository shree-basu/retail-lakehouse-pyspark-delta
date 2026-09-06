{{ config(materialized='table') }}

with sales as (
    select
        customer_id,
        count(distinct case when status = 'completed' then order_id end) as completed_orders,
        coalesce(sum(case when status = 'completed' then line_revenue else 0 end), 0) as lifetime_value,
        max(case when status = 'completed' then order_date end) as last_order_date
    from {{ ref('fct_order_items') }}
    group by customer_id
),
engagement as (
    select customer_id, count(*) as event_count, max(event_ts) as last_event_ts
    from {{ ref('stg_customer_events') }}
    group by customer_id
)
select
    c.customer_id,
    c.customer_name,
    c.country,
    c.segment,
    coalesce(s.completed_orders, 0) as completed_orders,
    cast(coalesce(s.lifetime_value, 0) as decimal(20, 2)) as lifetime_value,
    s.last_order_date,
    coalesce(e.event_count, 0) as event_count,
    e.last_event_ts
from {{ ref('stg_customers') }} c
left join sales s on c.customer_id = s.customer_id
left join engagement e on c.customer_id = e.customer_id
