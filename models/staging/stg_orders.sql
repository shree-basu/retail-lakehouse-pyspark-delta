select order_id, customer_id, order_ts, order_date, status, payment_method, currency, updated_at
from {{ source('silver', 'orders') }}
