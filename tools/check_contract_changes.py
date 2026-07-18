from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict

from kimball.cli import discover_config_paths
from kimball.common.config import ConfigLoader, TargetLoader
from kimball.contracts.gate import evaluate_contract_changes, load_contract_directory


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate producer contract evolution and consumer pins"
    )
    parser.add_argument("--current", required=True)
    parser.add_argument("--previous")
    parser.add_argument("--config", nargs="*")
    parser.add_argument("--target", required=True, choices=("dev", "test", "prod"))
    parser.add_argument("--targets", default="kimball.targets.yml")
    args = parser.parse_args()

    current = load_contract_directory(args.current)
    previous = load_contract_directory(args.previous) if args.previous else []
    result = evaluate_contract_changes(previous, current)
    if args.config:
        # ConfigLoader resolves every exact contract_ref. Project DAG validation is
        # performed separately because a repository may contain alternative demo
        # configurations that are not one deployable project.
        target = TargetLoader(args.targets).load(args.target)
        loader = ConfigLoader(
            env_vars=os.environ,
            template_context=target.template_context(),
        )
        for path in discover_config_paths(args.config):
            loader.load_config(path)

    print(
        json.dumps(
            {
                "allowed": result.allowed,
                "errors": list(result.errors),
                "reports": [asdict(report) for report in result.reports],
                "contracts_validated": len(current),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if result.allowed else 2


if __name__ == "__main__":
    raise SystemExit(main())
