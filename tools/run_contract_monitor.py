"""Run read-only data contract checks as a scheduled Databricks Job task."""

from __future__ import annotations

import argparse
import glob

from kimball.common.spark_session import get_spark
from kimball.orchestration.contracts_monitor import ContractMonitor


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Monitor Kimball upstream data contracts"
    )
    parser.add_argument("--config-glob", required=True)
    parser.add_argument("--etl-schema", required=True)
    args = parser.parse_args()
    result = ContractMonitor(
        glob.glob(args.config_glob, recursive=True), get_spark(), args.etl_schema
    ).run()
    print(result)
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
