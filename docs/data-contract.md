# Source data contract

## Delivery identity

```text
<input-root>/<business-date>/<batch-id>/
```

`business-date` is ISO `YYYY-MM-DD`. `batch-id` is a safe lowercase identifier and must equal the manifest value. Delivery directories are immutable: the simulator refuses to overwrite them.

## Required entities

| Entity | File | Format | Natural key |
|---|---|---|---|
| customers | `customers.csv` | CSV with exact header | `customer_id` |
| products | `products.csv` | CSV with exact header | `product_id` |
| orders | `orders.csv` | CSV with exact header | `order_id` |
| order_items | `order_items.csv` | CSV with exact header | `order_item_id` |
| customer_events | `customer_events.jsonl` | JSON Lines | `event_id` |

The implementation in `retail_lakehouse/contracts.py` is authoritative for field order and names.

## Manifest acceptance

The pipeline rejects a batch before Spark publication if any of these differ:

- schema version;
- manifest business date/batch ID versus directory path;
- exact entity set;
- declared filename or format;
- non-negative integer row count versus the physical file;
- SHA-256 checksum;
- CSV header versus the declared contract.

These are permanent producer-contract failures. Correct the delivery under a new batch ID; do not mutate the accepted immutable path.

## Supported evolution

`customer_events.attributes` is a string map for compatible additive event attributes. All top-level changes require a new schema version and coordinated Bronze/Silver/dbt migration. Automatic schema merge remains disabled so evolution cannot occur accidentally.
