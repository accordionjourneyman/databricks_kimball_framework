"""
Benchmark runner: orchestrates performance scenarios, collects metrics, writes reports.

Usage:
    python tools/benchmark_runner.py --scenario all --scale medium
    python tools/benchmark_runner.py --scenario scd2_change_detection --scale small
    python tools/benchmark_runner.py --scenario all --scale tiny small
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
import types
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

# Ensure src/ is importable
_HERE = Path(__file__).parent.parent
sys.path.insert(0, str(_HERE / "src"))
sys.path.insert(0, str(_HERE / "tools"))

if "databricks.sdk.runtime" not in sys.modules:
    mock_db_sdk = types.ModuleType("databricks.sdk.runtime")
    mock_db_sdk.spark = MagicMock()
    mock_db_sdk.dbutils = MagicMock()
    sys.modules["databricks.sdk.runtime"] = mock_db_sdk

from benchmark_data import (  # noqa: E402
    ScaleTier,
    create_benchmark_database,
    drop_benchmark_database,
    generate_customers,
    generate_orders,
    generate_products,
    make_changes,
)
from benchmark_metrics import (  # noqa: E402
    MetricsListener,
    capture_execution_plan,
    extract_exchanges,
    extract_join_types,
    extract_window_ops,
    profile_stage,
    save_metrics_report,
)
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import col  # noqa: E402

from kimball.orchestration.orchestrator import Orchestrator  # noqa: E402
from kimball.orchestration.transaction import TransactionManager  # noqa: E402
from kimball.orchestration.watermark import ETLControlManager  # noqa: E402
from kimball.processing.loader import DataLoader  # noqa: E402
from kimball.processing.skeleton_generator import SkeletonGenerator  # noqa: E402
from kimball.processing.table_creator import TableCreator  # noqa: E402

BENCHMARK_CONFIGS = {
    "scd1_baseline": "scd1_baseline.yml",
    "scd2_change_detection": "scd2_change_detection.yml",
    "scd2_full_cdc_delete": "scd2_full_cdc_delete.yml",
    "scd2_effective_at": "scd2_effective_at.yml",
    "multi_source_join": "multi_source_join.yml",
    "schema_evolution": "schema_evolution.yml",
    "identity_bridge": "identity_bridge.yml",
    "validation_overhead": "validation_overhead.yml",
}

SCALE_MAP = {
    "tiny": ScaleTier.tiny(),
    "small": ScaleTier.small(),
    "medium": ScaleTier.medium(),
    "large": ScaleTier.large(),
}


@dataclass
class ScenarioResult:
    """Collected metrics for a single scenario run."""
    scenario: str
    scale: str
    rows_in_target: int
    first_run_status: str
    second_run_status: str
    first_run_total_ms: float
    second_run_total_ms: float
    first_run_stages: list[dict] = field(default_factory=list)
    second_run_stages: list[dict] = field(default_factory=list)
    first_run_total_shuffle: int = 0
    second_run_total_shuffle: int = 0
    first_run_disk_spill: int = 0
    second_run_disk_spill: int = 0
    first_run_exchanges: int = 0
    second_run_exchanges: int = 0
    first_run_windows: int = 0
    second_run_windows: int = 0
    first_run_join_types: list[str] = field(default_factory=list)
    second_run_join_types: list[str] = field(default_factory=list)
    timestamp: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "scale": self.scale,
            "rows_in_target": self.rows_in_target,
            "first_run_status": self.first_run_status,
            "second_run_status": self.second_run_status,
            "first_run_total_ms": round(self.first_run_total_ms, 1),
            "second_run_total_ms": round(self.second_run_total_ms, 1),
            "first_run_total_shuffle_bytes": self.first_run_total_shuffle,
            "second_run_total_shuffle_bytes": self.second_run_total_shuffle,
            "first_run_disk_spill_bytes": self.first_run_disk_spill,
            "second_run_disk_spill_bytes": self.second_run_disk_spill,
            "first_run_exchanges": self.first_run_exchanges,
            "second_run_exchanges": self.second_run_exchanges,
            "first_run_windows": self.first_run_windows,
            "second_run_windows": self.second_run_windows,
            "first_run_join_types": self.first_run_join_types,
            "second_run_join_types": self.second_run_join_types,
            "first_run_stages": self.first_run_stages,
            "second_run_stages": self.second_run_stages,
            "timestamp": self.timestamp,
        }


def build_spark() -> SparkSession:
    """Build a local SparkSession tuned for benchmarks."""
    return (
        SparkSession.builder.appName("KimballBenchmark")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.ansi.enabled", "false")
        .config(
            "spark.sql.warehouse.dir",
            tempfile.mkdtemp(prefix="spark-warehouse-bench-"),
        )
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )


def render_config(template_path: Path, db: str) -> str:
    """Render a Jinja2 config template with the given database name."""
    from jinja2 import StrictUndefined
    from jinja2.sandbox import SandboxedEnvironment

    with open(template_path) as f:
        content = f.read()
    rendered = SandboxedEnvironment(undefined=StrictUndefined).from_string(content).render(db=db)
    return rendered


def write_config(content: str, tmp_dir: Path) -> Path:
    """Write a rendered config to a temp file and return the path."""
    path = tmp_dir / f"bench_{uuid.uuid4().hex[:8]}.yml"
    path.write_text(content, encoding="utf-8")
    return path


def run_orchestrator(
    config_path: str,
    spark: SparkSession,
    db: str,
    listener: MetricsListener,
) -> dict[str, Any]:
    """Run the orchestrator and collect metrics."""
    listener.reset()
    start = time.time()
    try:
        loader = DataLoader(spark_session=spark)
        etl_control = ETLControlManager(etl_schema=db, spark_session=spark)
        table_creator = TableCreator()
        skeleton_generator = SkeletonGenerator(spark)
        transaction_manager = TransactionManager(spark_session=spark)
        orchestrator = Orchestrator(
            config_path,
            spark=spark,
            etl_schema=db,
            loader=loader,
            etl_control=etl_control,
            table_creator=table_creator,
            skeleton_generator=skeleton_generator,
            transaction_manager=transaction_manager,
        )
        result = orchestrator.run()
        error = None
    except Exception as e:
        import traceback
        result = {"status": "ERROR", "error": str(e)}
        error = traceback.format_exc()
    duration_ms = (time.time() - start) * 1000

    if error:
        print(f"  ORCHESTRATOR ERROR:\n{error[:2000]}")

    return {
        "result": result,
        "duration_ms": duration_ms,
        "total_shuffle": 0,
        "disk_spill": 0,
        "stages": [],
    }


def setup_identity_bridge_data(spark: SparkSession, db: str, n_customers: int) -> None:
    """Create a bridge table for the identity_bridge scenario."""
    spark.sql(f"DROP TABLE IF EXISTS {db}.customer_bridge")
    spark.sql(f"""
        CREATE TABLE {db}.customer_bridge (
            customer_id INT,
            canonical_customer_id INT
        ) USING DELTA
    """)
    bridge_count = max(1, n_customers // 2)
    bridge_df = (
        spark.range(bridge_count)
        .select(
            col("id").cast("int").alias("customer_id"),
            (col("id") / 3).cast("int").alias("canonical_customer_id"),
        )
    )
    bridge_df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.customer_bridge")


def setup_schema_evolution(spark: SparkSession, db: str) -> None:
    """Add a new column to source to test schema evolution path."""
    spark.sql(f"ALTER TABLE {db}.products_src ADD COLUMN new_col STRING")
    spark.sql(f"UPDATE {db}.products_src SET new_col = 'added_later' WHERE product_id < 100")


def run_scenario(
    scenario: str,
    scale_name: str,
    scale: ScaleTier,
    spark: SparkSession,
    listener: MetricsListener,
    output_dir: Path,
) -> ScenarioResult:
    """Run a single benchmark scenario and return results."""
    print(f"\n{'='*60}")
    print(f"  Scenario: {scenario}  Scale: {scale_name}")
    print(f"{'='*60}")

    db = create_benchmark_database(spark)
    result = ScenarioResult(
        scenario=scenario,
        scale=scale_name,
        rows_in_target=0,
        first_run_status="",
        second_run_status="",
        first_run_total_ms=0.0,
        second_run_total_ms=0.0,
        timestamp=datetime.now().isoformat(),
    )

    try:
        # --- Data generation ---
        with profile_stage("data_generation") as prof:
            generate_products(spark, scale.products, db)
            generate_customers(spark, scale.customers, db)
            if scenario == "multi_source_join":
                generate_orders(spark, scale.orders, scale.customers, db)
                spark.sql(f"""
                    CREATE TABLE {db}.orders_src AS
                    SELECT order_id, customer_id, order_timestamp
                    FROM {db}.order_items_src
                    GROUP BY order_id, customer_id, order_timestamp
                """)
            if scenario == "identity_bridge":
                setup_identity_bridge_data(spark, db, scale.customers)
        print(f"  Data generation: {prof.duration_ms:.0f}ms")
        if scenario == "identity_bridge":
            spark.sql(
                f"INSERT INTO {db}.customer_bridge "
                f"SELECT customer_id, customer_id FROM {db}.customers_src "
                f"WHERE customer_id >= {scale.customers // 2}"
            )

        # Verify tables exist
        for tbl in [f"{db}.products_src", f"{db}.customers_src"]:
            if not spark.catalog.tableExists(tbl):
                print(f"  ERROR: Table {tbl} does not exist after generation!")
                result.first_run_status = "TABLE_MISSING"
                return result

        # --- Load config ---
        template_path = _HERE / "tools" / "benchmarks" / BENCHMARK_CONFIGS[scenario]
        config_text = render_config(template_path, db)
        config_path = write_config(config_text, output_dir)
        print(f"  Config (first 300 chars): {config_text[:300]}")

        # --- First run (initial load) ---
        first = run_orchestrator(str(config_path), spark, db, listener)
        result.first_run_status = first["result"].get("status", "UNKNOWN")
        result.first_run_total_ms = first["duration_ms"]
        result.first_run_total_shuffle = first["total_shuffle"]
        result.first_run_disk_spill = first["disk_spill"]
        result.first_run_stages = first["stages"]

        target_table_name = config_text.split('table_name:')[1].split()[0].strip()
        target_exists = spark.catalog.tableExists(target_table_name)
        print(
            f"  First run:  {first['duration_ms']:.0f}ms  "
            f"shuffle={first['total_shuffle']/1024/1024:.1f}MB  "
            f"status={result.first_run_status}  "
            f"target_exists={target_exists}"
        )

        if result.first_run_status != "SUCCESS" or not target_exists:
            print("  Skipping second run due to first run failure")
            return result

        # --- Mutate source (except for SCD1 baseline / multi_source_join) ---
        if scenario not in ("multi_source_join", "validation_overhead"):
            with profile_stage("data_mutation") as prof:
                make_changes(spark, scale, db)
            print(f"  Data mutation: {prof.duration_ms:.0f}ms")
        elif scenario == "validation_overhead":
            with profile_stage("data_mutation") as prof:
                spark.sql(f"""
                    UPDATE {db}.products_src
                    SET price = price * 1.10
                    WHERE product_id < {scale.n_changed}
                """)
            print(f"  Data mutation: {prof.duration_ms:.0f}ms")

        if scenario == "schema_evolution":
            setup_schema_evolution(spark, db)

        # --- Second run (incremental) ---
        second = run_orchestrator(str(config_path), spark, db, listener)
        result.second_run_status = second["result"].get("status", "UNKNOWN")
        result.second_run_total_ms = second["duration_ms"]
        result.second_run_total_shuffle = second["total_shuffle"]
        result.second_run_disk_spill = second["disk_spill"]
        result.second_run_stages = second["stages"]

        if scenario in ("scd2_change_detection", "scd2_full_cdc_delete", "scd2_effective_at",
                        "scd1_baseline", "schema_evolution", "validation_overhead",
                        "identity_bridge"):
            try:
                plan_excerpt = capture_execution_plan(
                    spark.read.format("delta").table(
                        f"{db}.{config_text.split('table_name:')[1].split('.')[1].split()[0]}"
                    ).limit(1)
                )
                result.second_run_join_types = extract_join_types(plan_excerpt)
                result.second_run_exchanges = extract_exchanges(plan_excerpt)
                result.second_run_windows = extract_window_ops(plan_excerpt)
            except Exception:
                pass
        print(
            f"  Second run: {second['duration_ms']:.0f}ms  "
            f"shuffle={second['total_shuffle']/1024/1024:.1f}MB  "
            f"status={result.second_run_status}"
        )

        # --- Row count ---
        try:
            result.rows_in_target = (
                spark.table(f"{db}.{config_text.split('table_name:')[1].split('.')[1].split()[0]}")
                .filter("__is_current = true")
                .count()
            )
        except Exception:
            pass

    finally:
        drop_benchmark_database(spark, db)

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Kimball framework benchmark runner")
    parser.add_argument(
        "--scenario",
        default="all",
        help=f"Scenario name or 'all'. Choices: {', '.join(BENCHMARK_CONFIGS.keys())}",
    )
    parser.add_argument(
        "--scale",
        nargs="+",
        default=["tiny"],
        help=f"Scale tier(s). Choices: {', '.join(SCALE_MAP.keys())}",
    )
    parser.add_argument(
        "--output-dir",
        default="tools/benchmarks/results",
        help="Output directory for results JSON",
    )
    args = parser.parse_args()

    scenarios = (
        list(BENCHMARK_CONFIGS.keys()) if args.scenario == "all" else [args.scenario]
    )
    scales = [SCALE_MAP[s] for s in args.scale if s in SCALE_MAP]
    if not scales:
        print(f"Invalid scale. Choices: {list(SCALE_MAP.keys())}")
        sys.exit(1)

    output_dir = Path(_HERE / args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    listener = MetricsListener()
    listener.attach(spark)

    all_results: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []

    try:
        for scale in scales:
            for scenario in scenarios:
                result = run_scenario(
                    scenario, scale.name, scale, spark, listener, output_dir
                )
                d = result.to_dict()
                all_results.append(d)
                summary_rows.append({
                    "scenario": scenario,
                    "scale": scale.name,
                    "first_ms": d["first_run_total_ms"],
                    "second_ms": d["second_run_total_ms"],
                    "first_shuffle_mb": round(d["first_run_total_shuffle_bytes"] / 1024 / 1024, 1),
                    "second_shuffle_mb": round(d["second_run_total_shuffle_bytes"] / 1024 / 1024, 1),
                    "first_spill_mb": round(d["first_run_disk_spill_bytes"] / 1024 / 1024, 1),
                    "second_spill_mb": round(d["second_run_disk_spill_bytes"] / 1024 / 1024, 1),
                    "first_exchanges": d["first_run_exchanges"],
                    "second_exchanges": d["second_run_exchanges"],
                    "first_status": d["first_run_status"],
                    "second_status": d["second_run_status"],
                })
    finally:
        spark.stop()

    # Write results
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = output_dir / f"benchmark_{timestamp_str}.json"
    save_metrics_report(str(report_path), {
        "timestamp": datetime.now().isoformat(),
        "scenarios": scenarios,
        "scales": [s.name for s in scales],
        "results": all_results,
    })

    summary_path = output_dir / f"summary_{timestamp_str}.json"
    save_metrics_report(str(summary_path), summary_rows)

    print(f"\n{'='*60}")
    print("  Results written to:")
    print(f"    Full report: {report_path}")
    print(f"    Summary:     {summary_path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
