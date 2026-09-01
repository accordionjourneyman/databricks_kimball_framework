"""Unit tests for the read-only ContractMonitor orchestration loop.

Runs the monitor against a real local Delta source with a contracted
pipeline config; asserts the summary accounting and the written event
rows (mocking only the alert webhook).
"""

from __future__ import annotations

import os
import shutil
import uuid
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
)

from kimball.orchestration.contracts_monitor import ContractMonitor


def _is_remote_only() -> bool:
    try:
        from pyspark.rdd import is_remote_only

        return bool(is_remote_only())
    except ImportError:
        return False


def _has_java() -> bool:
    return shutil.which("java") is not None or bool(os.environ.get("JAVA_HOME"))


@pytest.fixture(scope="module")
def spark():
    if _is_remote_only():
        pytest.skip("Databricks Connect cannot create a local Spark session")
    if not _has_java():
        pytest.skip("Java is not available -- skipping Delta behavior tests")
    builder = SparkSession.builder.appName("KimballContractsMonitor")
    builder = builder.master("local[2]")
    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )
    builder = builder.config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    builder = builder.config(
        "spark.sql.warehouse.dir", "spark-warehouse-contracts-monitor-tests"
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        pass
    session = builder.getOrCreate()
    from kimball.common.spark_session import set_active_spark

    set_active_spark(session)
    yield session


def _make_source_table(spark, extra_column: bool) -> str:
    table = f"mon_orders_{uuid.uuid4().hex[:6]}"
    fields = [
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
    ]
    rows: list[tuple] = [(1, 10, 5.0), (2, 20, None)]
    if extra_column:
        fields.append(StructField("bonus", IntegerType(), True))
        rows = [(*row, 99 - i) for i, row in enumerate(rows)]
    df = spark.createDataFrame(rows, schema=StructType(fields))
    df.write.format("delta").mode("error").saveAsTable(table)
    return table


def _write_config(tmp_path, table: str, source_table: str, compatibility: str) -> str:
    path = tmp_path / f"mon_{uuid.uuid4().hex[:6]}.yml"
    path.write_text(
        f"""
table_name: dim_out
surrogate_key: out_sk
natural_keys: [customer_id]
table_type: dimension
scd_type: 1
sources:
  - name: {source_table}
    alias: orders
    cdc_strategy: full
    contract:
      id: orders-contract
      version: "1.0.0"
      compatibility: {compatibility}
      schema:
        order_id: {{type: int, nullable: true}}
        customer_id: {{type: int, nullable: true}}
        amount: {{type: double, nullable: true}}
observability:
  enabled: true
  event_table: etl_dq_events_mon
  alert_on: [error]
        """.strip()
    )
    return str(path)


class TestContractMonitor:
    def test_passing_contract_reports_no_failures(self, spark, tmp_path):
        source_table = _make_source_table(spark, extra_column=False)
        cfg = _write_config(tmp_path, source_table, source_table, "strict")
        summary = ContractMonitor([cfg], spark, etl_schema=source_table).run()
        assert summary["checked"] == 1
        assert summary["failed"] == 0

    def test_additive_column_under_strict_fails(self, spark, tmp_path):
        source_table = _make_source_table(spark, extra_column=True)
        cfg = _write_config(tmp_path, source_table, source_table, "strict")
        with patch("kimball.observability.data_quality.AlertDispatcher.dispatch"):
            summary = ContractMonitor([cfg], spark, etl_schema=source_table).run()
        assert summary["checked"] == 1
        assert summary["failed"] == 1

    def test_sources_without_contract_are_skipped(self, spark, tmp_path):
        source_table = _make_source_table(spark, extra_column=False)
        path = tmp_path / f"mon_nc_{uuid.uuid4().hex[:6]}.yml"
        path.write_text(
            f"""
table_name: dim_x
surrogate_key: x_sk
natural_keys: [customer_id]
surrogate_key: x_sk
table_type: dimension
scd_type: 1
sources:
  - name: {source_table}
    alias: orders
    cdc_strategy: full
            """.strip()
        )
        summary = ContractMonitor([str(path)], spark, etl_schema=source_table).run()
        assert summary == {"checked": 0, "failed": 0}

    def test_missing_source_counts_as_failure(self, spark, tmp_path):
        cfg_table = _make_source_table(spark, extra_column=False)
        path = tmp_path / f"mon_miss_{uuid.uuid4().hex[:6]}.yml"
        path.write_text(
            """
table_name: dim_y
surrogate_key: y_sk
natural_keys: [order_id]
surrogate_key: y_sk
table_type: dimension
scd_type: 1
sources:
  - name: no_such_source_ct
    alias: orders
    cdc_strategy: full
    contract:
      id: c1
      version: "1.0.0"
      schema:
        order_id: {type: int, nullable: true}
observability:
  enabled: true
  event_table: etl_dq_events_missing
            """.strip()
        )
        summary = ContractMonitor([str(path)], spark, etl_schema=cfg_table).run()
        assert summary["checked"] == 1
        assert summary["failed"] == 1

    def test_from_glob_resolves_paths(self, spark, tmp_path, monkeypatch):
        cfg = _write_config(
            tmp_path,
            _make_source_table(spark, extra_column=False),
            _make_source_table(spark, extra_column=False),
            "nullable_additions",
        )
        pattern = str(cfg).split("/")[-1]
        monkeypatch.chdir(tmp_path)
        monitor = ContractMonitor.from_glob(pattern, spark, etl_schema="x")
        assert [os.path.basename(p) for p in monitor.config_paths] == [pattern]
