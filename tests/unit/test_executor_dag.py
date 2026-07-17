from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.common.config import SourceConfig, TableConfig
from kimball.common.errors import NonRetriableError
from kimball.orchestration.executor import PipelineExecutor, PipelineResult


def _dimension(name: str, depends_on: list[str] | None = None) -> TableConfig:
    return TableConfig(
        table_name=name,
        table_type="dimension",
        surrogate_key="dimension_sk",
        natural_keys=["id"],
        depends_on=depends_on or [],
        sources=[SourceConfig(name=f"silver.{name}", alias="src")],
    )


def _fact(name: str, depends_on: list[str]) -> TableConfig:
    return TableConfig(
        table_name=name,
        table_type="fact",
        merge_keys=["id"],
        depends_on=depends_on,
        sources=[SourceConfig(name="silver.sales", alias="src")],
    )


def _executor(configs: list[TableConfig]) -> PipelineExecutor:
    with (
        patch("kimball.orchestration.executor.ConfigLoader") as loader_cls,
        patch("kimball.orchestration.executor.get_etl_schema", return_value="etl"),
    ):
        loader_cls.return_value.load_config.side_effect = configs
        return PipelineExecutor(
            [f"config-{index}.yml" for index in range(len(configs))],
            profile="production",
        )


def test_executor_runs_compiled_levels_in_dependency_order():
    date = _dimension("gold.dim_date")
    customer = _dimension("gold.dim_customer", ["gold.dim_date"])
    sales = _fact("gold.fact_sales", ["gold.dim_customer", "gold.dim_date"])
    executor = _executor([sales, customer, date])
    order: list[str] = []

    def run(info):
        order.append(info["table_name"])
        return PipelineResult(
            info["path"], info["table_name"], info["table_type"], "SUCCESS"
        )

    executor._run_single_pipeline = MagicMock(side_effect=run)
    summary = executor.run()

    assert order == ["gold.dim_date", "gold.dim_customer", "gold.fact_sales"]
    assert summary.successful == 3


def test_executor_skips_only_failed_downstream_branch():
    left = _dimension("gold.dim_left")
    right = _dimension("gold.dim_right")
    left_fact = _fact("gold.fact_left", ["gold.dim_left"])
    right_fact = _fact("gold.fact_right", ["gold.dim_right"])
    executor = _executor([left, right, left_fact, right_fact])

    def run(info):
        status = "FAILED" if info["table_name"] == "gold.dim_left" else "SUCCESS"
        return PipelineResult(
            info["path"], info["table_name"], info["table_type"], status
        )

    executor._run_single_pipeline = MagicMock(side_effect=run)
    summary = executor.run()
    statuses = {result.table_name: result.status for result in summary.results}

    assert statuses == {
        "gold.dim_left": "FAILED",
        "gold.dim_right": "SUCCESS",
        "gold.fact_left": "SKIPPED",
        "gold.fact_right": "SUCCESS",
    }


def test_in_process_parallel_method_is_safely_serialized():
    executor = _executor([])
    executor._run_sequential = MagicMock(return_value=[])

    with pytest.warns(RuntimeWarning, match="share a Spark session"):
        result = executor._run_parallel([{"table_name": "gold.dim_date"}])

    assert result == []
    executor._run_sequential.assert_called_once()


def test_executor_rejects_missing_upstream_before_spark_work():
    config = _fact("gold.fact_sales", ["gold.dim_missing"])

    with pytest.raises(NonRetriableError, match="Invalid pipeline project"):
        _executor([config])
