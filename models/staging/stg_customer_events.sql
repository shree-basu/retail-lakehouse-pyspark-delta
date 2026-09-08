select
    event_id,
    customer_id,
    event_ts,
    event_date,
    event_type,
    page,
    device_type,
    device_os,
    attributes,
    updated_at
from {{ source('silver', 'customer_events') }}
