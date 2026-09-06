{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_date',
    cluster_by=['order_date']
) }}

{% if is_incremental() %}
with changed_dates as (
    select distinct order_date
    from {{ ref('fct_order_items') }}
    where source_updated_at >= (
        select coalesce(max(source_updated_at), cast('1900-01-01' as timestamp)) from {{ this }}
    )
),
{% else %}
with
{% endif %}
daily as (
    select
        order_date,
        count(distinct order_id) as order_count,
        sum(quantity) as items_sold,
        cast(sum(line_revenue) as decimal(20, 2)) as revenue,
        max(source_updated_at) as source_updated_at
    from {{ ref('fct_order_items') }}
    where status = 'completed'
    {% if is_incremental() %}
      and order_date in (select order_date from changed_dates)
    {% endif %}
    group by order_date
)
select * from daily
