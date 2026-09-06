select oi.order_item_id
from {{ ref('stg_order_items') }} oi
inner join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
where oi.currency <> o.currency
