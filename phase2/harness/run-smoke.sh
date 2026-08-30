#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE=(docker compose -f "$HARNESS_DIR/docker-compose.yml")

"${COMPOSE[@]}" up -d --build --wait
"${COMPOSE[@]}" exec -T dbt dbt debug --profiles-dir .
"${COMPOSE[@]}" exec -T dbt dbt seed --profiles-dir . --full-refresh
"${COMPOSE[@]}" exec -T dbt dbt run --profiles-dir . --vars '{"upper_bound": 2}' --full-refresh
"${COMPOSE[@]}" exec -T dbt dbt run --profiles-dir . --vars '{"upper_bound": 3}'
"${COMPOSE[@]}" exec -T dbt dbt test --profiles-dir .
"${COMPOSE[@]}" exec -T dbt python tools/validate_smoke.py
"${COMPOSE[@]}" exec -T framework \
  /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_4.0_2.13:4.2.0 \
  --conf spark.jars.ivy=/opt/spark/ivy \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.hive.metastore.sharedPrefixes=org.postgresql,org.apache.spark.sql.delta,io.delta \
  --driver-class-path /opt/spark/jars/postgresql-42.7.5.jar \
  /work/read_dbt_delta.py
