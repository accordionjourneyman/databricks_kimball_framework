"""Differential checks for the dbt and framework Phase 2 outputs."""


from __future__ import annotations

import json
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


state = sys.argv[1]
spark = (
    SparkSession.builder.appName(f"phase2-reconcile-{state}")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

tables = {
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
report: dict[str, object] = {"state": state, "tables": {}}
for table, columns in tables.items():
    left = spark.table(f"dbt_reference.{table}").select(*columns)
    right = spark.table(f"framework_result.{table}").select(*columns)
    left_only = left.exceptAll(right).count()
    right_only = right.exceptAll(left).count()
    left_types = [(field.name, field.dataType.simpleString()) for field in left.schema]
    right_types = [(field.name, field.dataType.simpleString()) for field in right.schema]
    schema_match = left_types == right_types
    duplicate_keys = 0
    if table == "fact_sales":
        duplicate_keys = (
            left.groupBy("source_system", "line_id")
            .count()
            .where(F.col("count") > 1)
            .count()
        )
    details = {
        "dbt_rows": left.count(),
        "framework_rows": right.count(),
        "dbt_schema": left.schema.simpleString(),
        "framework_schema": right.schema.simpleString(),
        "dbt_nullable": {field.name: field.nullable for field in left.schema},
        "framework_nullable": {field.name: field.nullable for field in right.schema},
        "schema_match": schema_match,
        "dbt_only": left_only,
        "framework_only": right_only,
        "duplicate_keys": duplicate_keys,
        "deterministic_sample": [
            row.asDict(recursive=True)
            for row in left.orderBy(*columns[:2]).limit(3).collect()
        ],
        "classification": "equivalent"
        if left_only == right_only == duplicate_keys == 0 and schema_match
        else "different",
    }
    if table in {"dim_product", "fact_sales"}:
        details["dbt_delta_version"] = (
            DeltaTable.forName(spark, f"dbt_reference.{table}")
            .history(1)
            .select("version")
            .first()["version"]
        )
        details["framework_delta_version"] = (
            DeltaTable.forName(spark, f"framework_result.{table}")
            .history(1)
            .select("version")
            .first()["version"]
        )
    report["tables"][table] = details

customer_checks: dict[str, object] = {}
for implementation, relation in {
    "dbt": "dbt_reference.dim_customer",
    "framework": "framework_result.dim_customer",
}.items():
    customers = spark.table(relation)
    overlap_count = (
        customers.alias("earlier")
        .join(
            customers.alias("later"),
            (F.col("earlier.source_system") == F.col("later.source_system"))
            & (F.col("earlier.customer_id") == F.col("later.customer_id"))
            & (F.col("earlier.valid_from") < F.col("later.valid_from"))
            & (F.col("earlier.valid_to") > F.col("later.valid_from")),
        )
        .count()
    )
    current_violations = (
        customers.where("is_current")
        .groupBy("source_system", "customer_id")
        .count()
        .where(F.col("count") != 1)
        .count()
    )
    customer_checks[implementation] = {
        "temporal_overlaps": overlap_count,
        "current_member_violations": current_violations,
    }

fact_checks: dict[str, object] = {}
for implementation, relation in {
    "dbt": "dbt_reference.fact_sales",
    "framework": "framework_result.fact_sales",
}.items():
    facts = spark.table(relation)
    fact_checks[implementation] = {
        "line_amount_total": str(
            facts.agg(F.sum("line_amount").alias("total")).first()["total"]
        ),
        "customer_resolution_counts": {
            row["customer_resolution"]: row["count"]
            for row in facts.groupBy("customer_resolution").count().collect()
        },
        "missing_product_references": facts.where(
            F.col("stock_code") == "MISSING"
        ).count(),
    }

report["invariants"] = {
    "dim_customer": customer_checks,
    "fact_sales": fact_checks,
}
if any(
    values["temporal_overlaps"] or values["current_member_violations"]
    for values in customer_checks.values()
):
    report["tables"]["dim_customer"]["classification"] = "different"
if fact_checks["dbt"] != fact_checks["framework"]:
    report["tables"]["fact_sales"]["classification"] = "different"

failed = [
    name
    for name, result in report["tables"].items()
    if result["classification"] != "equivalent"
]
output = Path("/reference/evidence") / f"reconciliation-{state}.json"
output.parent.mkdir(exist_ok=True)
output.write_text(json.dumps(report, indent=2, default=str, sort_keys=True) + "\n")
print(
    f"PHASE2_RECONCILIATION={json.dumps(report, default=str, sort_keys=True)}"
)
spark.stop()
if failed:
    raise SystemExit(f"unexplained differences: {', '.join(failed)}")
