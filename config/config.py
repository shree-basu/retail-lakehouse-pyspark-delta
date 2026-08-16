from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_ROOT = PROJECT_ROOT / "data"

BRONZE_PATH = DATA_ROOT / "bronze"
SILVER_PATH = DATA_ROOT / "silver"
GOLD_PATH = DATA_ROOT / "gold"

RAW_DATA_PATH = DATA_ROOT / "raw"

CUSTOMER_TABLE = "customers"
ORDER_TABLE = "orders"
ORDER_ITEM_TABLE = "order_items"
PRODUCT_TABLE = "products"

CHURN_LABEL_TABLE = "customer_churn"
