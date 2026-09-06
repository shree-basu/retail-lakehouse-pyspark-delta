select product_id, product_name, category, unit_price, updated_at, _ingested_at
from {{ source('silver', 'products') }}
