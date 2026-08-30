"""Shared test data fixtures for unit and integration tests.

Loads small CSV files from ``tests/data/`` into Spark DataFrames.
Every function here takes ``spark`` and returns a DataFrame with the
standard schema documented in each function's docstring.

Usage anywhere a ``SparkSession`` is available::

    from tests.data import customers, orders

    def test_something(spark: SparkSession):
        cust = customers(spark)
        ordr = orders(spark)
        ...
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

_DATA = Path(__file__).resolve().parent


def customers(spark: SparkSession) -> DataFrame:
    """Load the customer dimension fixture.

    Schema: customer_id INT, customer_name STRING, email STRING,
            country STRING, updated_at STRING (ISO date).
    Rows: 10/Alice/Portugal, 20/Bob/Spain, 30/Charlie/Portugal.
    """
    return spark.read.option("header", True).csv(str(_DATA / "customers.csv"))


def orders(spark: SparkSession) -> DataFrame:
    """Load the orders fact fixture.

    Schema: order_id INT, customer_id INT, amount DOUBLE,
            order_date STRING (ISO date), status STRING.
    Rows: 1/10/100.0, 2/20/200.0, 3/10/50.0.
    """
    return spark.read.option("header", True).csv(str(_DATA / "orders.csv"))


# ---------------------------------------------------------------------------
# Integration-test helpers — create Delta tables from shared fixtures.
# These produce the standard Kimball dimension/fact-source schemas used by
# the integration tests in tests/integration/.
# ---------------------------------------------------------------------------


def create_dim_customer_table(
    spark: SparkSession, test_db: str, *, include_skeleton_cols: bool = True
) -> None:
    """Create and populate ``{test_db}.dim_customer`` from shared fixture data.

    Surrogate keys are deterministic (1, 2, 3) ordered by ``customer_id``.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    sk = F.row_number().over(Window.orderBy("customer_id")).cast("bigint")
    sel = [
        sk.alias("customer_sk"),
        F.col("customer_id"),
        F.col("customer_name").alias("name"),
        F.lit(True).alias("__is_current"),
        F.current_timestamp().alias("__valid_from"),
        F.lit(None).cast("timestamp").alias("__valid_to"),
        F.current_timestamp().alias("__etl_processed_at"),
        F.lit("INIT").alias("__etl_batch_id"),
    ]
    if include_skeleton_cols:
        sel += [
            F.lit(False).alias("__is_skeleton"),
            F.lit(None).cast("timestamp").alias("__skeleton_created_at"),
            F.lit(False).alias("__is_deleted"),
        ]

    customers(spark).select(*sel).write.mode("overwrite").format("delta").saveAsTable(
        f"{test_db}.dim_customer"
    )


def create_orders_src_table(spark: SparkSession, test_db: str) -> None:
    """Create and populate ``{test_db}.orders_src`` from shared fixture data.

    Schema: order_id INT, customer_id INT, amount DOUBLE.
    """
    orders(spark).select("order_id", "customer_id", "amount").write.mode(
        "overwrite"
    ).format("delta").saveAsTable(f"{test_db}.orders_src")
