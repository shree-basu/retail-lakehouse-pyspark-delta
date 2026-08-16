from pyspark.sql import SparkSession

from src.silver import clean_customers, clean_products


def create_test_spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("RetailLakehouse-Tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def test_clean_customers_removes_duplicates():
    spark = create_test_spark()

    data = [
        (1, " Alice ", "alice@example.com", "2024-01-01", "US", "Standard"),
        (1, " Alice ", "alice@example.com", "2024-01-01", "US", "Standard"),
        (2, " Bob ", "bob@example.com", "2024-02-01", "UK", "Premium"),
    ]

    columns = [
        "customer_id",
        "customer_name",
        "email",
        "signup_date",
        "country",
        "segment",
    ]

    df = spark.createDataFrame(data, columns)

    result = clean_customers(df)

    assert result.count() == 2

    spark.stop()


def test_clean_products_standardizes_values():
    spark = create_test_spark()

    data = [
        (1, " Laptop ", " ELECTRONICS ", 1000.50),
    ]

    columns = [
        "product_id",
        "product_name",
        "category",
        "unit_price",
    ]

    df = spark.createDataFrame(data, columns)

    result = clean_products(df)

    row = result.first()

    assert row.product_id == 1
    assert row.product_name == "Laptop"
    assert row.category == "electronics"
    assert row.unit_price == 1000.50

    spark.stop()
