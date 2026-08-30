"""Cross-client catalog/Delta read proof for Phase 2.0."""


from __future__ import annotations

import json

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder.appName("kimball-phase2-cross-client")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", "/opt/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

table = "dbt_baseline.smoke_incremental"
rows = [row.asDict(recursive=True) for row in spark.table(table).orderBy("id").collect()]
history = [
    row.asDict(recursive=True)
    for row in spark.sql(f"DESCRIBE HISTORY {table}")
    .select("version", "operation")
    .orderBy("version")
    .collect()
]
result = {"table": table, "rows": rows, "history": history}
print(
    f"PHASE2_CROSS_CLIENT_PROOF={json.dumps(result, default=str, sort_keys=True)}"
)
assert rows == [
    {"id": 1, "value": "alpha"},
    {"id": 2, "value": "beta"},
    {"id": 3, "value": "gamma"},
]
spark.stop()
