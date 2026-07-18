from __future__ import annotations

import argparse
import glob
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

import yaml
from jsonschema import ValidationError as JsonSchemaValidationError

from kimball.common.config import ConfigLoader, TargetConfig, TargetLoader
from kimball.contracts.compatibility import check_compatibility
from kimball.contracts.odcs import ODCSContractLoader
from kimball.planning.bundle import build_bundle_job
from kimball.planning.compiler import CompiledProject, Profile, ProjectCompiler
from kimball.planning.manifest import build_manifest, diff_manifests, manifest_json


def discover_config_paths(inputs: Sequence[str]) -> list[str]:
    """Resolve files, directories, and glob expressions deterministically."""

    discovered: set[str] = set()
    for value in inputs:
        path = Path(value)
        if path.is_dir():
            candidates = [*path.rglob("*.yml"), *path.rglob("*.yaml")]
        elif path.is_file():
            candidates = [path]
        elif glob.has_magic(value):
            candidates = [Path(match) for match in glob.glob(value, recursive=True)]
        else:
            # Preserve the path so ConfigLoader supplies the actionable error.
            candidates = [path]
        discovered.update(str(candidate) for candidate in candidates)
    return sorted(discovered)


def load_target(name: str, path: str) -> TargetConfig:
    return TargetLoader(path).load(name)


def _profile_for_target(target: TargetConfig) -> Profile:
    profiles: dict[str, Profile] = {
        "dev": "dev",
        "test": "test",
        "prod": "production",
    }
    try:
        return profiles[target.name]
    except KeyError as exc:
        raise ValueError(
            f"Target '{target.name}' is not supported. Use one of: dev, test, prod"
        ) from exc


def load_compiled_project(
    inputs: Sequence[str], target: TargetConfig
) -> CompiledProject:
    paths = discover_config_paths(inputs)
    if not paths:
        raise ValueError("No YAML pipeline configurations were found")
    loader = ConfigLoader(template_context=target.template_context())
    entries = [(path, loader.load_config(path)) for path in paths]
    return ProjectCompiler(profile=_profile_for_target(target)).compile(entries)


def _write_text(path_value: str, content: str) -> None:
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _add_project_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", nargs="+", required=True)
    parser.add_argument("--target", required=True, choices=("dev", "test", "prod"))
    parser.add_argument("--targets", default="kimball.targets.yml")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kimball")
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate", help="Validate and compile a project")
    _add_project_arguments(validate)

    compile_command = commands.add_parser(
        "compile", help="Create a deterministic project manifest"
    )
    _add_project_arguments(compile_command)
    compile_command.add_argument("--output")
    compile_command.add_argument("--bundle-output")
    compile_command.add_argument("--job-name", default="kimball_compiled_job")

    plan = commands.add_parser("plan", help="Diff a project against a manifest")
    _add_project_arguments(plan)
    plan.add_argument("--against", required=True)
    plan.add_argument("--fail-on-breaking", action="store_true")

    run = commands.add_parser("run", help="Run one pipeline configuration")
    run.add_argument("--config", required=True)
    run.add_argument("--target", required=True, choices=("dev", "test", "prod"))
    run.add_argument("--targets", default="kimball.targets.yml")

    contract = commands.add_parser(
        "contract", help="Validate and publish ODCS contracts"
    )
    contract_commands = contract.add_subparsers(dest="contract_command", required=True)
    contract_validate = contract_commands.add_parser("validate")
    contract_validate.add_argument("--contract", nargs="+", required=True)
    contract_check = contract_commands.add_parser("check")
    contract_check.add_argument("--previous", required=True)
    contract_check.add_argument("--current", required=True)
    contract_check.add_argument("--fail-on-breaking", action="store_true")
    contract_publish = contract_commands.add_parser("publish")
    contract_publish.add_argument("--contract", required=True)
    contract_publish.add_argument("--etl-schema", required=True)
    contract_publish.add_argument("--published-by")

    manifest = commands.add_parser("manifest", help="Publish deployment manifests")
    manifest_commands = manifest.add_subparsers(dest="manifest_command", required=True)
    manifest_publish = manifest_commands.add_parser("publish")
    manifest_publish.add_argument("--manifest", required=True)
    manifest_publish.add_argument("--etl-schema", required=True)
    manifest_publish.add_argument("--environment", required=True)
    manifest_publish.add_argument("--source-revision")
    manifest_publish.add_argument("--deployed-by")
    return parser


def _validate(args: argparse.Namespace) -> int:
    target = load_target(args.target, args.targets)
    project = load_compiled_project(args.config, target)
    for warning in project.warnings:
        print(f"WARNING {warning}")
    print(f"Validated {len(project.nodes)} pipelines in {len(project.levels)} levels")
    return 0


def _compile(args: argparse.Namespace) -> int:
    target = load_target(args.target, args.targets)
    project = load_compiled_project(args.config, target)
    manifest = build_manifest(project)
    rendered = manifest_json(manifest)
    if args.output:
        _write_text(args.output, rendered)
    else:
        print(rendered, end="")
    if args.bundle_output:
        bundle = build_bundle_job(
            project, job_name=args.job_name, target_name=target.name
        )
        _write_text(args.bundle_output, yaml.safe_dump(bundle, sort_keys=False))
    return 0


def _plan(args: argparse.Namespace) -> int:
    target = load_target(args.target, args.targets)
    project = load_compiled_project(args.config, target)
    previous = json.loads(Path(args.against).read_text(encoding="utf-8"))
    plan = diff_manifests(previous, build_manifest(project))
    print(
        json.dumps(
            {
                "changes": [asdict(change) for change in plan.changes],
                "affected_tables": list(plan.affected_tables),
                "has_breaking_changes": plan.has_breaking_changes,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 2 if args.fail_on_breaking and plan.has_breaking_changes else 0


def _run(args: argparse.Namespace) -> int:
    # Import lazily so planning commands remain light and side-effect free.
    from kimball.orchestration.orchestrator import Orchestrator

    target = load_target(args.target, args.targets)
    config = ConfigLoader(template_context=target.template_context()).load_config(
        args.config
    )
    result = Orchestrator(
        config,
        etl_schema=target.etl_schema,
        checkpoint_root=target.checkpoint_root,
    ).run()
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


def _contract_validate(args: argparse.Namespace) -> int:
    loader = ODCSContractLoader()
    for path in args.contract:
        contract = loader.load_file(path)
        print(
            f"Validated {contract.id} {contract.version} "
            f"({len(contract.objects)} schema objects)"
        )
    return 0


def _contract_check(args: argparse.Namespace) -> int:
    loader = ODCSContractLoader()
    report = check_compatibility(
        loader.load_file(args.previous), loader.load_file(args.current)
    )
    print(
        json.dumps(
            {
                "allowed": report.allowed,
                "requires_consumer_migration": report.requires_consumer_migration,
                "version_errors": list(report.version_errors),
                "changes": [asdict(change) for change in report.changes],
            },
            indent=2,
            sort_keys=True,
        )
    )
    if not report.allowed or (args.fail_on_breaking and report.breaking_changes):
        return 2
    return 0


def _contract_publish(args: argparse.Namespace) -> int:
    from kimball.common.spark_session import get_spark
    from kimball.contracts.registry import DeltaContractRegistry

    contract = ODCSContractLoader().load_file(args.contract)
    created = DeltaContractRegistry(get_spark(), args.etl_schema).publish_contract(
        contract,
        source_path=args.contract,
        published_by=args.published_by,
    )
    action = "Published" if created else "Already published"
    print(f"{action} {contract.id} {contract.version}")
    return 0


def _manifest_publish(args: argparse.Namespace) -> int:
    from kimball.common.spark_session import get_spark
    from kimball.contracts.registry import DeltaContractRegistry

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    legacy = [
        pipeline["table_name"]
        for pipeline in manifest.get("pipelines", [])
        if pipeline.get("semantic_config", {}).get("null_policy", {}).get("mode")
        == "legacy"
    ]
    if legacy:
        raise ValueError(
            "Legacy null policy cannot be published: " + ", ".join(sorted(legacy))
        )
    registry = DeltaContractRegistry(get_spark(), args.etl_schema)
    registry.publish_manifest(
        manifest,
        environment=args.environment,
        source_revision=args.source_revision,
        deployed_by=args.deployed_by,
    )
    for pipeline in manifest.get("pipelines", []):
        for source in pipeline.get("semantic_config", {}).get("sources", []):
            contract = source.get("contract")
            if source.get("contract_ref") and contract:
                registry.register_consumer(
                    pipeline_table=pipeline["table_name"],
                    source_table=source["name"],
                    contract_id=contract["id"],
                    contract_version=contract["version"],
                    config_digest=pipeline["semantic_digest"],
                )
    print(f"Published manifest {manifest['project_digest']} to {args.environment}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "validate":
            return _validate(args)
        if args.command == "compile":
            return _compile(args)
        if args.command == "plan":
            return _plan(args)
        if args.command == "run":
            return _run(args)
        if args.command == "contract":
            if args.contract_command == "validate":
                return _contract_validate(args)
            if args.contract_command == "check":
                return _contract_check(args)
            if args.contract_command == "publish":
                return _contract_publish(args)
        if args.command == "manifest" and args.manifest_command == "publish":
            return _manifest_publish(args)
    except (OSError, ValueError, JsonSchemaValidationError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
