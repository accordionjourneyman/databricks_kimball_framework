"""Independent PySpark implementation of the Phase 2 target contract."""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


max_event_seq = int(sys.argv[1])
spark = (
    SparkSession.builder.appName(f"phase2-framework-state-{max_event_seq}")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", "/opt/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.sql("CREATE DATABASE IF NOT EXISTS framework_result")

events = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("/reference/fixtures/retail_events.csv")
    .where(F.col("event_seq") <= max_event_seq)
    .withColumn("event_seq", F.col("event_seq").cast("bigint"))
    .withColumn("quantity", F.col("quantity").cast("bigint"))
    .withColumn("customer_id", F.col("customer_id").cast("bigint"))
    .withColumn("invoice_ts", F.to_timestamp("invoice_ts"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
)

product_window = Window.partitionBy("source_system", "stock_code").orderBy(
    F.col("event_seq").desc()
)
products = (
    events.where(F.col("stock_code").isNotNull() & (F.col("stock_code") != "MISSING"))
    .withColumn("_rank", F.row_number().over(product_window))
    .where(F.col("_rank") == 1)
    .select(
        "source_system",
        "stock_code",
        "description",
        "unit_price",
        F.col("event_seq").alias("last_event_seq"),
    )
)
products.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("framework_result.dim_product")

customer_window = Window.partitionBy("source_system", "customer_id").orderBy(
    "invoice_ts", "event_seq"
)
same_time_window = Window.partitionBy(
    "source_system", "customer_id", "invoice_ts"
).orderBy(F.col("event_seq").desc())
customers = (
    events.where(F.col("customer_id").isNotNull() & F.col("country").isNotNull())
    .withColumn("same_time_rank", F.row_number().over(same_time_window))
    .where(F.col("same_time_rank") == 1)
    .withColumn("valid_from", F.col("invoice_ts"))
    .withColumn("valid_to_raw", F.lead("invoice_ts").over(customer_window))
    .withColumn(
        "valid_to",
        F.coalesce("valid_to_raw", F.to_timestamp(F.lit("9999-12-31 23:59:59"))),
    )
    .withColumn("is_current", F.col("valid_to_raw").isNull())
    .select(
        "source_system",
        "customer_id",
        "country",
        "customer_segment",
        "valid_from",
        "valid_to",
        "is_current",
    )
)
customers.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("framework_result.dim_customer")

fact_replay_window = Window.partitionBy("source_system", "line_id").orderBy(
    F.col("event_seq").desc()
)
fact_events = (
    events.where(F.col("event_type").isin("sale", "cancellation"))
    .withColumn("_replay_rank", F.row_number().over(fact_replay_window))
    .where(F.col("_replay_rank") == 1)
)
facts = (
    fact_events
    .alias("e")
    .join(
        customers.alias("c"),
        (F.col("e.source_system") == F.col("c.source_system"))
        & (F.col("e.customer_id") == F.col("c.customer_id"))
        & (F.col("e.invoice_ts") >= F.col("c.valid_from"))
        & (F.col("e.invoice_ts") < F.col("c.valid_to")),
        "left",
    )
    .select(
        "e.source_system",
        "e.line_id",
        "e.invoice_no",
        "e.stock_code",
        "e.customer_id",
        "e.invoice_ts",
        "e.quantity",
        "e.unit_price",
        (F.col("e.quantity") * F.col("e.unit_price"))
        .cast("decimal(18,2)")
        .alias("line_amount"),
        (F.col("e.event_type") == "cancellation").alias("is_cancellation"),
        F.when(F.col("e.customer_id").isNull(), "UNKNOWN")
        .when(F.col("c.customer_id").isNull(), "SKELETON")
        .otherwise("MATCHED")
        .alias("customer_resolution"),
    )
)
facts.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("framework_result.fact_sales")

spark.stop()
