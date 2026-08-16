from pathlib import Path
import csv
import random
from datetime import datetime, timedelta


OUTPUT_DIR = Path(__file__).resolve().parent / "raw"

RANDOM_SEED = 42
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_ORDERS = 5000

random.seed(RANDOM_SEED)


def generate_customers():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    path = OUTPUT_DIR / "customers.csv"

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow([
            "customer_id",
            "customer_name",
            "email",
            "signup_date",
            "country",
            "segment",
        ])

        for customer_id in range(1, NUM_CUSTOMERS + 1):
            signup_date = datetime(2023, 1, 1) + timedelta(
                days=random.randint(0, 730)
            )

            segment = random.choices(
                ["standard", "premium", "enterprise"],
                weights=[70, 25, 5],
            )[0]

            writer.writerow([
                customer_id,
                f"Customer {customer_id}",
                f"customer{customer_id}@example.com",
                signup_date.date(),
                random.choice(["US", "UK", "DE", "IN", "CA"]),
                segment,
            ])

    return path


def generate_products():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    path = OUTPUT_DIR / "products.csv"

    categories = [
        "electronics",
        "home",
        "fashion",
        "beauty",
        "sports",
    ]

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow([
            "product_id",
            "product_name",
            "category",
            "unit_price",
        ])

        for product_id in range(1, NUM_PRODUCTS + 1):
            writer.writerow([
                product_id,
                f"Product {product_id}",
                random.choice(categories),
                round(random.uniform(10, 1000), 2),
            ])

    return path


def generate_orders():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    path = OUTPUT_DIR / "orders.csv"

    start_date = datetime(2024, 1, 1)

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow([
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "payment_method",
        ])

        for order_id in range(1, NUM_ORDERS + 1):
            order_date = start_date + timedelta(
                days=random.randint(0, 365)
            )

            writer.writerow([
                order_id,
                random.randint(1, NUM_CUSTOMERS),
                order_date.date(),
                random.choice([
                    "completed",
                    "completed",
                    "completed",
                    "cancelled",
                ]),
                random.choice([
                    "card",
                    "paypal",
                    "bank_transfer",
                ]),
            ])

    return path


def generate_order_items():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    path = OUTPUT_DIR / "order_items.csv"

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow([
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
        ])

        order_item_id = 1

        for order_id in range(1, NUM_ORDERS + 1):
            number_of_items = random.randint(1, 5)

            for _ in range(number_of_items):
                product_id = random.randint(1, NUM_PRODUCTS)
                unit_price = round(random.uniform(10, 1000), 2)

                writer.writerow([
                    order_item_id,
                    order_id,
                    product_id,
                    random.randint(1, 4),
                    unit_price,
                ])

                order_item_id += 1

    return path


def main():
    generated_files = [
        generate_customers(),
        generate_products(),
        generate_orders(),
        generate_order_items(),
    ]

    print("Generated retail source datasets:")

    for path in generated_files:
        print(f"- {path}")


if __name__ == "__main__":
    main()

