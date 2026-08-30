"""
Runner script for Databricks integration tests.

Supports two modes:
  1. Direct test path: python run_tests.py tests/integration catalog [extra args]
  2. Sharded mode:     python run_tests.py --shard <name> catalog [extra args]

Shards split the test suite into balanced groups for parallel execution
across multiple job tasks sharing a single serverless compute session.
"""

import os
import sys

_SHARD_MAP = {
    "scd": [
        "tests/integration/test_scd_integration.py",
        "tests/integration/test_scd2_single_pass_regression.py",
        "tests/integration/test_scd7_key_broker.py",
        "tests/integration/test_skeleton_generation.py",
        "tests/integration/test_schema_evolution.py",
    ],
    "services": [
        "tests/integration/test_resolution_validation.py",
        "tests/integration/test_junk_dimension_and_descriptions.py",
        "tests/integration/test_pii_tokenization.py",
        "tests/integration/test_temporal_contract_state.py",
        "tests/integration/test_streaming_cdf.py",
    ],
    "datasets": [
        "tests/integration/test_nyc_taxi.py",
        "tests/integration/test_olist.py",
        "tests/integration/test_online_retail.py",
        "tests/integration/test_contract_observability.py",
        "tests/integration/test_synthea.py",
        "tests/benchmarks/test_framework_benchmarks.py",
        "tests/benchmarks/test_scd2_single_pass.py",
        "tests/benchmarks/test_hypothesis_scaling.py",
    ],
    "all": ["tests/integration", "tests/benchmarks"],
}


def _resolve_test_paths() -> tuple[list[str], str, list[str]]:
    if len(sys.argv) > 1 and sys.argv[1] == "--shard":
        shard_name = sys.argv[2] if len(sys.argv) > 2 else "all"
        catalog = sys.argv[3] if len(sys.argv) > 3 else "spark_catalog"
        extra = sys.argv[4:]
        tests = _SHARD_MAP.get(shard_name)
        if tests is None:
            print(f"Unknown shard '{shard_name}'. Available: {', '.join(_SHARD_MAP)}")
            sys.exit(1)
        return tests, catalog, extra

    test_path = sys.argv[1] if len(sys.argv) > 1 else "tests/integration"
    catalog = sys.argv[2] if len(sys.argv) > 2 else "spark_catalog"
    extra_args = sys.argv[3:] if len(sys.argv) > 3 else []
    return [test_path], catalog, extra_args


def main() -> int:
    test_paths, catalog, extra_args = _resolve_test_paths()
    os.environ.setdefault("KIMBALL_TEST_CATALOG", catalog)
    os.environ.setdefault("DATABRICKS_HOST", "")
    os.environ.setdefault("DATABRICKS_TOKEN", "")
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    sys.dont_write_bytecode = True

    print(f"Shard: {test_paths}")
    print(f"Catalog: {catalog}")

    import pytest
    return pytest.main(test_paths + ["-v", "-p", "no:cacheprovider"] + extra_args)


if __name__ == "__main__":
    if _exit_code := main():
        raise SystemExit(_exit_code)
