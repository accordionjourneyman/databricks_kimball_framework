"""Scaling benchmark datasets and tests for optimization hypotheses."""

import pytest

from tests.benchmarks.test_framework_benchmarks import _measure


@pytest.mark.parametrize(
    "optimize_lazy_eval", [False, True], ids=["lazy_eval_off", "lazy_eval_on"]
)
@pytest.mark.parametrize(
    "approx_grain", [False, True], ids=["approx_grain_off", "approx_grain_on"]
)
@pytest.mark.parametrize(
    "disable_manual_pruning", [False, True], ids=["pruning_on", "pruning_disabled"]
)
def test_scd2_hypothesis_scaling(
    benchmark,
    spark,
    bench_db,
    tmp_path,
    scale_params,
    benchmark_rounds,
    benchmark_warmups,
    monkeypatch,
    optimize_lazy_eval,
    approx_grain,
    disable_manual_pruning,
):
    """
    Benchmarks the Databricks Kimball Framework with hypothesis flags at various scales.
    Use --scale=[tiny|small|medium] to control row counts (1K, 100K, 1M).
    """
    monkeypatch.setenv(
        "KIMBALL_OPTIMIZE_SCD2_LAZY_EVAL", "1" if optimize_lazy_eval else "0"
    )
    monkeypatch.setenv("KIMBALL_APPROX_GRAIN_CHECK", "1" if approx_grain else "0")
    monkeypatch.setenv(
        "KIMBALL_DISABLE_MANUAL_PRUNING", "1" if disable_manual_pruning else "0"
    )

    # Run a standard SCD2 measured test with 'changed' phase
    _measure(
        benchmark,
        spark,
        bench_db,
        tmp_path,
        scale_params,
        benchmark_rounds,
        benchmark_warmups,
        scd_type=2,
        phase="changed",
    )


@pytest.mark.parametrize(
    "batch_control_writes", [False, True], ids=["batch_writes_off", "batch_writes_on"]
)
def test_etl_control_hypothesis(
    benchmark,
    spark,
    bench_db,
    tmp_path,
    scale_params,
    benchmark_rounds,
    benchmark_warmups,
    monkeypatch,
    batch_control_writes,
):
    """
    Benchmarks the overhead of ETL control table synchronous serial writes vs asynchronous/batched.
    """
    monkeypatch.setenv(
        "KIMBALL_BATCH_CONTROL_WRITES", "1" if batch_control_writes else "0"
    )

    _measure(
        benchmark,
        spark,
        bench_db,
        tmp_path,
        scale_params,
        benchmark_rounds,
        benchmark_warmups,
        scd_type=1,
        phase="initial",
    )
