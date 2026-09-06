select order_item_id, order_id, product_id, order_date, quantity, unit_price, currency, updated_at
from {{ source('silver', 'order_items') }}
