select order_date, revenue
from {{ ref('agg_daily_sales') }}
where revenue < 0
