"""
Synthetic data generators for performance benchmarks.

Generates Delta tables at various scales to stress-test the framework.
Uses range partitioning via spark.range for fast generation.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    current_timestamp,
    date_add,
    lit,
    rand,
    randn,
    when,
)


@dataclass
class ScaleTier:
    """Defines data volume at a given scale."""

    name: str
    products: int
    customers: int
    orders: int
    order_items: int
    n_changed: int
    n_deleted: int
    n_new: int

    @classmethod
    def tiny(cls) -> ScaleTier:
        return cls(
            name="tiny_1k",
            products=100,
            customers=500,
            orders=1_000,
            order_items=3_000,
            n_changed=300,
            n_deleted=100,
            n_new=100,
        )

    @classmethod
    def small(cls) -> ScaleTier:
        return cls(
            name="small_100k",
            products=10_000,
            customers=50_000,
            orders=100_000,
            order_items=300_000,
            n_changed=30_000,
            n_deleted=10_000,
            n_new=10_000,
        )

    @classmethod
    def medium(cls) -> ScaleTier:
        return cls(
            name="medium_1m",
            products=100_000,
            customers=500_000,
            orders=1_000_000,
            order_items=3_000_000,
            n_changed=300_000,
            n_deleted=100_000,
            n_new=100_000,
        )

    @classmethod
    def large(cls) -> ScaleTier:
        return cls(
            name="large_10m",
            products=1_000_000,
            customers=5_000_000,
            orders=10_000_000,
            order_items=30_000_000,
            n_changed=3_000_000,
            n_deleted=1_000_000,
            n_new=1_000_000,
        )


def generate_products(spark: SparkSession, n: int, db: str) -> None:
    """Create a products dimension with 10 tracked columns."""
    spark.sql(f"DROP TABLE IF EXISTS {db}.products_src")
    spark.sql(f"""
        CREATE TABLE {db}.products_src (
            product_id INT,
            name STRING,
            price DECIMAL(10,2),
            category_id INT,
            brand STRING,
            color STRING,
            size STRING,
            weight_g INT,
            in_stock BOOLEAN,
            launch_date DATE,
            rating DOUBLE
        ) USING DELTA
    """)
    df = spark.range(n).select(
        col("id").cast("int").alias("product_id"),
        concat(lit("Product_"), col("id").cast("string")).alias("name"),
        (rand() * 1000).cast("decimal(10,2)").alias("price"),
        (rand() * 100).cast("int").alias("category_id"),
        concat(lit("Brand_"), (rand() * 50).cast("int").cast("string")).alias("brand"),
        when(rand() < 0.3, lit("red"))
        .when(rand() < 0.6, lit("blue"))
        .otherwise(lit("green"))
        .alias("color"),
        when(rand() < 0.5, lit("S"))
        .when(rand() < 0.8, lit("M"))
        .otherwise(lit("L"))
        .alias("size"),
        (rand() * 5000).cast("int").alias("weight_g"),
        (rand() > 0.2).alias("in_stock"),
        date_add(lit("2024-01-01").cast("date"), (rand() * 1000).cast("int")).alias(
            "launch_date"
        ),
        (rand() * 5).alias("rating"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.products_src")


def generate_customers(spark: SparkSession, n: int, db: str) -> None:
    """Create a customers dimension with 8 tracked columns."""
    spark.sql(f"DROP TABLE IF EXISTS {db}.customers_src")
    spark.sql(f"""
        CREATE TABLE {db}.customers_src (
            customer_id INT,
            name STRING,
            email STRING,
            country STRING,
            city STRING,
            age INT,
            segment STRING,
            lifetime_value DECIMAL(12,2)
        ) USING DELTA
    """)
    df = spark.range(n).select(
        col("id").cast("int").alias("customer_id"),
        concat(lit("Customer_"), col("id").cast("string")).alias("name"),
        concat(lit("user"), col("id").cast("string"), lit("@example.com")).alias(
            "email"
        ),
        when(rand() < 0.4, lit("US"))
        .when(rand() < 0.7, lit("UK"))
        .when(rand() < 0.85, lit("DE"))
        .otherwise(lit("FR"))
        .alias("country"),
        concat(lit("City_"), (rand() * 1000).cast("int").cast("string")).alias("city"),
        (randn() * 15 + 40).cast("int").alias("age"),
        when(rand() < 0.6, lit("retail"))
        .when(rand() < 0.9, lit("wholesale"))
        .otherwise(lit("vip"))
        .alias("segment"),
        (rand() * 50000).cast("decimal(12,2)").alias("lifetime_value"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.customers_src")


def generate_orders(
    spark: SparkSession,
    n: int,
    n_customers: int,
    db: str,
    n_items_per_order: int = 3,
) -> None:
    """Create an orders fact table with n_items_per_order line items per order."""
    spark.sql(f"DROP TABLE IF EXISTS {db}.order_items_src")
    spark.sql(f"""
        CREATE TABLE {db}.order_items_src (
            order_id BIGINT,
            order_item_id INT,
            customer_id INT,
            product_id INT,
            quantity INT,
            unit_price DECIMAL(10,2),
            discount DOUBLE,
            order_timestamp TIMESTAMP
        ) USING DELTA
    """)
    orders_df = spark.range(n).select(
        col("id").alias("order_id"),
        (rand() * n_customers).cast("int").alias("customer_id"),
        current_timestamp().alias("order_timestamp"),
    )
    items_df = spark.range(n_items_per_order).select(
        col("id").cast("int").alias("order_item_id"),
    )
    df = orders_df.crossJoin(items_df).select(
        col("order_id"),
        col("order_item_id"),
        col("customer_id"),
        (rand() * 100000).cast("int").alias("product_id"),
        (rand() * 10 + 1).cast("int").alias("quantity"),
        (rand() * 500 + 10).cast("decimal(10,2)").alias("unit_price"),
        rand().alias("discount"),
        col("order_timestamp"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.order_items_src")


def make_changes(
    spark: SparkSession,
    scale: ScaleTier,
    db: str,
) -> None:
    """Mutate the source tables to simulate an incremental run:
    - Update n_changed products (price change)
    - Insert n_new products
    - Delete n_deleted products (full CDC detection)
    """
    new_price_expr = (
        f"CASE WHEN product_id < {scale.n_changed} THEN price * 1.15 ELSE price END"
    )
    spark.sql(f"UPDATE {db}.products_src SET price = {new_price_expr}")

    new_ids = spark.range(100_000_000, 100_000_000 + scale.n_new).select(
        col("id").cast("int").alias("product_id"),
        concat(lit("NewProduct_"), col("id").cast("string")).alias("name"),
        (rand() * 1000).cast("decimal(10,2)").alias("price"),
        (rand() * 100).cast("int").alias("category_id"),
        lit("Brand_New").alias("brand"),
        lit("red").alias("color"),
        lit("M").alias("size"),
        (rand() * 5000).cast("int").alias("weight_g"),
        lit(True).alias("in_stock"),
        date_add(lit("2024-01-01").cast("date"), (rand() * 1000).cast("int")).alias(
            "launch_date"
        ),
        (rand() * 5).alias("rating"),
    )
    new_ids.write.format("delta").mode("append").saveAsTable(f"{db}.products_src")
    spark.sql(f"DELETE FROM {db}.products_src WHERE product_id < {scale.n_deleted}")


def create_benchmark_database(spark: SparkSession) -> str:
    """Create a unique benchmark database and return its name."""
    db = f"kimball_bench_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    return db


def drop_benchmark_database(spark: SparkSession, db: str) -> None:
    """Drop the benchmark database and all its tables."""
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
