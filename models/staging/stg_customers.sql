select customer_id, customer_name, email, signup_date, country, segment, updated_at
from {{ source('silver', 'customers') }}
