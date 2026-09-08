from src.gold import build_customer_sales, build_product_sales


def test_build_customer_sales(spark):
    customers_data = [
        (1, "Alice", "US", "premium"),
        (2, "Bob", "UK", "standard"),
    ]

    customers_columns = [
        "customer_id",
        "customer_name",
        "country",
        "segment",
    ]

    customers = spark.createDataFrame(
        customers_data,
        customers_columns,
    )

    orders_data = [
        (101, 1, "2024-12-01", "completed", "paypal"),
        (102, 1, "2024-12-05", "completed", "card"),
        (103, 2, "2024-12-06", "cancelled", "card"),
    ]

    orders_columns = [
        "order_id",
        "customer_id",
        "order_date",
        "status",
        "payment_method",
    ]

    orders = spark.createDataFrame(orders_data, orders_columns).withColumn(
        "order_date", __import__("pyspark.sql.functions").sql.functions.to_date("order_date")
    )

    order_items_data = [
        (1, 101, 10, 2, 100.0),
        (2, 101, 11, 1, 50.0),
        (3, 102, 10, 1, 100.0),
        (4, 103, 11, 2, 50.0),
    ]

    order_items_columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ]

    order_items = spark.createDataFrame(
        order_items_data,
        order_items_columns,
    )

    result = build_customer_sales(
        customers,
        orders,
        order_items,
    )

    alice = result.filter("customer_id = 1").first()
    bob = result.filter("customer_id = 2").first()

    assert alice.total_orders == 2
    assert alice.total_items == 4
    assert alice.total_spend == 350.0
    assert alice.avg_order_value == 175.0

    assert bob.total_orders == 0
    assert bob.total_spend == 0.0


def test_build_product_sales(spark):
    products_data = [
        (10, "Laptop", "electronics"),
        (11, "Mouse", "accessories"),
    ]

    products_columns = [
        "product_id",
        "product_name",
        "category",
    ]

    products = spark.createDataFrame(
        products_data,
        products_columns,
    )

    orders_data = [
        (101, 1, "2024-12-01", "completed", "paypal"),
        (102, 1, "2024-12-05", "cancelled", "card"),
    ]

    orders_columns = [
        "order_id",
        "customer_id",
        "order_date",
        "status",
        "payment_method",
    ]

    orders = spark.createDataFrame(
        orders_data,
        orders_columns,
    )

    order_items_data = [
        (1, 101, 10, 2, 100.0),
        (2, 101, 11, 1, 50.0),
        (3, 102, 10, 5, 100.0),
    ]

    order_items_columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ]

    order_items = spark.createDataFrame(
        order_items_data,
        order_items_columns,
    )

    result = build_product_sales(
        products,
        orders,
        order_items,
    )

    laptop = result.filter("product_id = 10").first()
    mouse = result.filter("product_id = 11").first()

    assert laptop.units_sold == 2
    assert laptop.revenue == 200.0

    assert mouse.units_sold == 1
    assert mouse.revenue == 50.0
