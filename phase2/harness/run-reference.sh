#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE=(docker compose -f "$HARNESS_DIR/docker-compose.yml")
SPARK=(
  /opt/spark/bin/spark-submit
  --packages io.delta:delta-spark_4.0_2.13:4.2.0
  --conf spark.jars.ivy=/opt/spark/ivy
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
  --conf spark.sql.catalog.spark_catalog.type=hive
  --conf spark.sql.hive.metastore.sharedPrefixes=org.postgresql,org.apache.spark.sql.delta,io.delta
  --conf spark.ui.showConsoleProgress=false
  --driver-class-path /opt/spark/jars/postgresql-42.7.5.jar
)

"${COMPOSE[@]}" up -d --build --wait
"${COMPOSE[@]}" exec -T spark-thrift \
  /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 \
  -e "DROP DATABASE IF EXISTS dbt_reference_snapshots CASCADE;
      DROP DATABASE IF EXISTS dbt_reference CASCADE;
      DROP DATABASE IF EXISTS dbt_reference_source CASCADE;
      DROP DATABASE IF EXISTS framework_result CASCADE;"
"${COMPOSE[@]}" exec -T dbt \
  dbt seed --project-dir /reference --profiles-dir /reference --full-refresh

states=(
  baseline:3
  dimension_changes:5
  incremental:6
  replay:7
  late_dimension:8
  late_fact:9
  unknowns:10
  duplicates:11
  orphans:12
  identity:13
  cancellation:14
)
for entry in "${states[@]}"; do
  state="${entry%%:*}"
  max_event_seq="${entry##*:}"
  run_args=()
  if [[ "$state" == "baseline" ]]; then
    run_args+=(--full-refresh)
  fi
  "${COMPOSE[@]}" exec -T dbt \
    dbt run --project-dir /reference --profiles-dir /reference \
      "${run_args[@]}" --vars "{\"max_event_seq\": $max_event_seq}"
  "${COMPOSE[@]}" exec -T dbt \
    dbt snapshot --project-dir /reference --profiles-dir /reference \
      --vars "{\"max_event_seq\": $max_event_seq}"
  "${COMPOSE[@]}" exec -T dbt \
    dbt test --project-dir /reference --profiles-dir /reference \
      --vars "{\"max_event_seq\": $max_event_seq}"
  "${COMPOSE[@]}" exec -T framework \
    "${SPARK[@]}" /reference/framework/run_reference.py "$max_event_seq"
  "${COMPOSE[@]}" exec -T framework \
    "${SPARK[@]}" /reference/reconcile.py "$state"
done
