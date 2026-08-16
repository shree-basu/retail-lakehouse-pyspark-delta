from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.config import BRONZE_PATH, SILVER_PATH
from src.spark_session import create_spark_session


def read_bronze_table(spark, table_name: str) -> DataFrame:
    """Read a Delta table from the Bronze layer."""
    return (
        spark.read
        .format("delta")
        .load(str(BRONZE_PATH / table_name))
    )


def clean_customers(df: DataFrame) -> DataFrame:
    """Clean and standardize customer records."""

    return (
        df
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("customer_name", F.trim(F.col("customer_name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .withColumn("segment", F.lower(F.trim(F.col("segment"))))
        .withColumn("signup_date", F.to_date("signup_date"))
        .dropDuplicates(["customer_id"])
    )

def clean_products(df: DataFrame) -> DataFrame:
    """Clean and standardize product records."""

    return (
        df
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .dropDuplicates(["product_id"])
    )


def clean_orders(df: DataFrame) -> DataFrame:
    """Clean and standardize order records."""

    return (
        df
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn(
            "payment_method",
            F.lower(F.trim(F.col("payment_method")))
        )
        .dropDuplicates(["order_id"])
    )

def clean_order_items(df: DataFrame) -> DataFrame:
    """Clean and standardize order item records."""

    return (
        df
        .withColumn("order_item_id", F.col("order_item_id").cast("long"))
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("quantity", F.col("quantity").cast("long"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .dropDuplicates(["order_item_id"])
    )






def write_silver_table(df: DataFrame, table_name: str) -> None:
    """Write a DataFrame as a Silver Delta table."""

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(SILVER_PATH / table_name))
    )


def run_silver_transformation():
    spark = create_spark_session("RetailLakehouse-Silver")

    customers = read_bronze_table(spark, "customers")
    customers_silver = clean_customers(customers)

    write_silver_table(customers_silver, "customers")
    print("Silver transformation completed: customers")

    products = read_bronze_table(spark, "products")
    products_silver = clean_products(products)

    write_silver_table(products_silver, "products")
    print("Silver transformation completed: products")

    orders = read_bronze_table(spark, "orders")
    orders_silver = clean_orders(orders)

    write_silver_table(orders_silver, "orders")
    print("Silver transformation completed: orders")

    order_items = read_bronze_table(spark, "order_items")
    order_items_silver = clean_order_items(order_items)

    write_silver_table(order_items_silver, "order_items")
    print("Silver transformation completed: order_items")


    spark.stop()




if __name__ == "__main__":
    run_silver_transformation()

