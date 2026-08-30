"""Serverless framework build and reconciliation for Phase 2.6."""


from __future__ import annotations

import json
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


catalog, dbt_schema, framework_schema = sys.argv[1:4]
spark = SparkSession.builder.appName("phase2-databricks-conformance").getOrCreate()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{framework_schema}`")

events = (
    spark.table(f"`{catalog}`.`{dbt_schema}_source`.`retail_events`")
    .where(F.col("event_seq") <= 14)
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

same_time_window = Window.partitionBy(
    "source_system", "customer_id", "invoice_ts"
).orderBy(F.col("event_seq").desc())
customer_window = Window.partitionBy("source_system", "customer_id").orderBy(
    "invoice_ts", "event_seq"
)
customers = (
    events.where(F.col("customer_id").isNotNull() & F.col("country").isNotNull())
    .withColumn("_same_time_rank", F.row_number().over(same_time_window))
    .where(F.col("_same_time_rank") == 1)
    .withColumn("valid_from", F.col("invoice_ts"))
    .withColumn("_valid_to", F.lead("invoice_ts").over(customer_window))
    .withColumn(
        "valid_to",
        F.coalesce("_valid_to", F.to_timestamp(F.lit("9999-12-31 23:59:59"))),
    )
    .withColumn("is_current", F.col("_valid_to").isNull())
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

fact_window = Window.partitionBy("source_system", "line_id").orderBy(
    F.col("event_seq").desc()
)
fact_events = (
    events.where(F.col("event_type").isin("sale", "cancellation"))
    .withColumn("_replay_rank", F.row_number().over(fact_window))
    .where(F.col("_replay_rank") == 1)
)
facts = (
    fact_events.alias("e")
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

outputs = {
    "dim_product": products,
    "dim_customer": customers,
    "fact_sales": facts,
}
for table, frame in outputs.items():
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"`{catalog}`.`{framework_schema}`.`{table}`")
    )

columns = {
    "dim_product": [
        "source_system",
        "stock_code",
        "description",
        "unit_price",
        "last_event_seq",
    ],
    "dim_customer": [
        "source_system",
        "customer_id",
        "country",
        "customer_segment",
        "valid_from",
        "valid_to",
        "is_current",
    ],
    "fact_sales": [
        "source_system",
        "line_id",
        "invoice_no",
        "stock_code",
        "customer_id",
        "invoice_ts",
        "quantity",
        "unit_price",
        "line_amount",
        "is_cancellation",
        "customer_resolution",
    ],
}
report: dict[str, object] = {
    "catalog": catalog,
    "dbt_schema": dbt_schema,
    "framework_schema": framework_schema,
    "tables": {},
}
for table, selected_columns in columns.items():
    dbt_frame = spark.table(f"`{catalog}`.`{dbt_schema}`.`{table}`").select(
        *selected_columns
    )
    framework_frame = spark.table(
        f"`{catalog}`.`{framework_schema}`.`{table}`"
    ).select(*selected_columns)
    dbt_only = dbt_frame.exceptAll(framework_frame).count()
    framework_only = framework_frame.exceptAll(dbt_frame).count()
    dbt_types = [
        (field.name, field.dataType.simpleString()) for field in dbt_frame.schema
    ]
    framework_types = [
        (field.name, field.dataType.simpleString())
        for field in framework_frame.schema
    ]
    report["tables"][table] = {
        "dbt_rows": dbt_frame.count(),
        "framework_rows": framework_frame.count(),
        "dbt_only": dbt_only,
        "framework_only": framework_only,
        "types_match": dbt_types == framework_types,
        "classification": "equivalent"
        if dbt_only == framework_only == 0 and dbt_types == framework_types
        else "different",
    }

print(f"PHASE2_DATABRICKS_CONFORMANCE={json.dumps(report, sort_keys=True)}")
if any(
    value["classification"] != "equivalent"
    for value in report["tables"].values()
):
    raise SystemExit("Databricks conformance differences detected")
