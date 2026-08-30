from __future__ import annotations

import argparse
import datetime
import glob
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

import yaml
from jsonschema import ValidationError as JsonSchemaValidationError

from kimball.common.config import ConfigLoader, TargetConfig, TargetLoader
from kimball.common.errors import KimballError
from kimball.contracts.compatibility import check_compatibility
from kimball.contracts.odcs import ODCSContractLoader
from kimball.ops.errors import ErrorCategory, StructuredError
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
        raise StructuredError(
            f"Target '{target.name}' is not supported. Use one of: dev, test, prod",
            category=ErrorCategory.CONFIG,
            remediation="Use --target dev|test|prod (defined in kimball.targets.yml).",
        ) from exc


def load_compiled_project(
    inputs: Sequence[str], target: TargetConfig
) -> CompiledProject:
    paths = discover_config_paths(inputs)
    if not paths:
        raise StructuredError(
            "No YAML pipeline configurations were found",
            category=ErrorCategory.CONFIG,
            remediation="Check that --config points to .yml/.yaml files or a directory containing them.",
        )
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
    validate.set_defaults(handler=_validate)

    compile_command = commands.add_parser(
        "compile", help="Create a deterministic project manifest"
    )
    _add_project_arguments(compile_command)
    compile_command.add_argument("--output")
    compile_command.add_argument("--bundle-output")
    compile_command.add_argument("--job-name", default="kimball_compiled_job")
    compile_command.set_defaults(handler=_compile)

    plan = commands.add_parser("plan", help="Diff a project against a manifest")
    _add_project_arguments(plan)
    plan.add_argument("--against", required=True)
    plan.add_argument("--fail-on-breaking", action="store_true")
    plan.set_defaults(handler=_plan)

    run = commands.add_parser("run", help="Run one pipeline configuration")
    run.add_argument("--config", required=True)
    run.add_argument("--target", required=True, choices=("dev", "test", "prod"))
    run.add_argument("--targets", default="kimball.targets.yml")
    run.set_defaults(handler=_run)

    contract = commands.add_parser(
        "contract", help="Validate and publish ODCS contracts"
    )
    contract_commands = contract.add_subparsers(dest="contract_command", required=True)
    contract_validate = contract_commands.add_parser("validate")
    contract_validate.add_argument("--contract", nargs="+", required=True)
    contract_validate.set_defaults(handler=_contract_validate)
    contract_check = contract_commands.add_parser("check")
    contract_check.add_argument("--previous", required=True)
    contract_check.add_argument("--current", required=True)
    contract_check.add_argument("--fail-on-breaking", action="store_true")
    contract_check.set_defaults(handler=_contract_check)
    contract_publish = contract_commands.add_parser("publish")
    contract_publish.add_argument("--contract", required=True)
    contract_publish.add_argument("--etl-schema", required=True)
    contract_publish.add_argument("--published-by")
    contract_publish.set_defaults(handler=_contract_publish)

    manifest = commands.add_parser("manifest", help="Publish deployment manifests")
    manifest_commands = manifest.add_subparsers(dest="manifest_command", required=True)
    manifest_publish = manifest_commands.add_parser("publish")
    manifest_publish.add_argument("--manifest", required=True)
    manifest_publish.add_argument("--etl-schema", required=True)
    manifest_publish.add_argument("--environment", required=True)
    manifest_publish.add_argument("--source-revision")
    manifest_publish.add_argument("--deployed-by")
    manifest_publish.set_defaults(handler=_manifest_publish)
    inspect = commands.add_parser(
        "inspect", help="Inspect target state, zombies and source health"
    )
    inspect.add_argument("--target", required=True, choices=("dev", "test", "prod"))
    inspect.add_argument("--targets", default="kimball.targets.yml")
    inspect.add_argument("--table", required=True)
    inspect.add_argument("--history-limit", type=int, default=200)
    inspect.add_argument(
        "--running", action="store_true", help="Show only RUNNING batches"
    )
    inspect.add_argument(
        "--failed", action="store_true", help="Show only FAILED batches"
    )
    inspect.add_argument(
        "--older-than",
        type=int,
        default=0,
        help="Min age in minutes for --running (default 0 = no filter)",
    )
    inspect.add_argument(
        "--json", action="store_true", help="JSON output (default for CLI)"
    )
    inspect.add_argument("--limit", type=int, default=50, help="Max rows (default 50)")
    inspect.set_defaults(handler=_inspect)

    recover = commands.add_parser(
        "recover", help="Recover a zombie/stale target (two-phase)"
    )
    recover.add_argument("--target", required=True, choices=("dev", "test", "prod"))
    recover.add_argument("--targets", default="kimball.targets.yml")
    recover.add_argument("--table", required=True)
    recover.add_argument("--batch-id")
    recover.add_argument("--dry-run", action="store_true")
    recover.add_argument("--rewind-watermark", action="store_true")
    recover.add_argument("--full-reload", action="store_true")
    recover.add_argument("--version", type=int)
    recover.add_argument("--timestamp")
    recover.add_argument("--force", action="store_true")
    recover.add_argument("--config")
    recover.set_defaults(handler=_recover)

    explain = commands.add_parser(
        "explain", help="Explain a target failure / dangerous state"
    )
    explain.add_argument("--target", required=True, choices=("dev", "test", "prod"))
    explain.add_argument("--targets", default="kimball.targets.yml")
    explain.add_argument("--table")
    explain.add_argument("--batch-id")
    explain.add_argument("--config")
    explain.set_defaults(handler=_explain)

    deploy = commands.add_parser(
        "deploy", help="Promotion gate: manifest diff + pre-flight before deploy"
    )
    _add_project_arguments(deploy)
    deploy.add_argument(
        "--against", default=None, help="Previous manifest (omit for first deploy)"
    )
    deploy.add_argument("--allow-breaking", action="store_true")
    deploy.set_defaults(handler=_deploy)

    qh = commands.add_parser(
        "query-history",
        help="Analyze system.query.history for the Kimball Framework",
    )
    qh.add_argument("--days", type=int, default=7, help="Days of history (default: 7)")
    qh.add_argument(
        "--warehouse", default=None, help="Warehouse ID (default: first running)"
    )
    qh.add_argument("--json", action="store_true", help="JSON output")
    qh.add_argument(
        "--verbose", action="store_true", help="Show full SQL for each query"
    )
    qh.set_defaults(handler=_query_history)

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
    previous = (
        json.loads(Path(args.against).read_text(encoding="utf-8"))
        if args.against
        else {"pipelines": []}
    )
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
    from kimball.orchestration.runtime import PipelineRuntime

    target = load_target(args.target, args.targets)
    config = ConfigLoader(template_context=target.template_context()).load_config(
        args.config
    )
    runtime = PipelineRuntime.for_config(
        config,
        etl_schema=target.etl_schema,
        checkpoint_root=target.checkpoint_root,
    )
    result = Orchestrator(config, runtime).run()
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
    if legacy := [
        pipeline["table_name"]
        for pipeline in manifest.get("pipelines", [])
        if pipeline.get("semantic_config", {}).get("null_policy", {}).get("mode")
        == "legacy"
    ]:
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


def _ops_runtime_and_providers(args: argparse.Namespace):
    from kimball.common.spark_session import get_spark
    from kimball.ops.runtime_profile import detect_runtime_profile
    from kimball.ops.spark_adapters import build_providers

    target = load_target(args.target, args.targets)
    spark = get_spark()
    runtime = detect_runtime_profile(spark)
    providers = build_providers(spark, target.etl_schema)
    return runtime, providers


def _inspect(args: argparse.Namespace) -> int:
    from kimball.ops.inspect import inspect_target

    runtime, providers = _ops_runtime_and_providers(args)
    report = inspect_target(
        args.table, providers, runtime, history_limit=args.history_limit
    )
    # Apply --running/--failed/--older-than filter to batch list
    if args.running or args.failed:
        now = datetime.datetime.now(datetime.timezone.utc)

        def _allowed(b: dict) -> bool:
            if args.running and b.get("status") != "RUNNING":
                return False
            if args.failed and b.get("status") != "FAILED":
                return False
            if args.running and args.older_than > 0:
                s = b.get("started_at")
                if s and hasattr(s, "tzinfo"):
                    if s.tzinfo is None:
                        s = s.replace(tzinfo=datetime.timezone.utc)
                    age = (now - s).total_seconds() / 60
                    if age < args.older_than:
                        return False
            return True

        report["batches"] = [b for b in report["batches"] if _allowed(b)]
    report["batches"] = report["batches"][: args.limit]
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0 if report["reconciliation"]["verdict"] == "consistent" else 1


def _recover(args: argparse.Namespace) -> int:
    from datetime import datetime

    from kimball.ops.recover import recover_target

    runtime, providers = _ops_runtime_and_providers(args)

    timestamp = None
    if args.timestamp:
        try:
            timestamp = datetime.fromisoformat(args.timestamp)
        except ValueError as exc:
            print(f"ERROR: --timestamp must be ISO-8601: {exc}", file=sys.stderr)
            return 1

    upstream_targets: tuple[str, ...] = ()
    if args.config:
        target = load_target(args.target, args.targets)
        project = load_compiled_project(args.config, target)
        if node := project.nodes.get(args.table):
            upstream_targets = tuple(node.dependencies)

    result = recover_target(
        args.table,
        providers,
        runtime,
        batch_id=args.batch_id,
        dry_run=args.dry_run,
        rewind_only=args.rewind_watermark,
        full_reload=args.full_reload,
        version=args.version,
        timestamp=timestamp,
        force=args.force,
        upstream_targets=upstream_targets,
    )
    print(json.dumps(result.to_dict(), indent=2, sort_keys=True, default=str))
    return 1 if result.partial else 0


def _explain(args: argparse.Namespace) -> int:
    from kimball.ops.explain import explain, explain_config_error

    if args.config and not args.table:
        try:
            target = load_target(args.target, args.targets)
            load_compiled_project(args.config, target)
            print(
                json.dumps(
                    {"entry_point": "config", "category": "OK", "verdict": "config-ok"}
                )
            )
            return 0
        except Exception as exc:  # noqa: BLE001 - any compile/config failure
            report = explain_config_error(
                exc, args.config[0] if isinstance(args.config, list) else None
            )
            print(json.dumps(report.to_dict(), indent=2, sort_keys=True, default=str))
            return 1

    if not args.table:
        print("ERROR: --table or --config is required for explain", file=sys.stderr)
        return 1

    runtime, providers = _ops_runtime_and_providers(args)

    current_config_fingerprint = None
    if args.config:
        target = load_target(args.target, args.targets)
        loader = ConfigLoader(template_context=target.template_context())
        for path in discover_config_paths(args.config):
            cfg = loader.load_config(path)
            if cfg.table_name == args.table:
                current_config_fingerprint = loader.compute_fingerprint(cfg)
                break

    report = explain(
        args.table,
        providers,
        runtime,
        batch_id=args.batch_id,
        current_config_fingerprint=current_config_fingerprint,
    )
    print(json.dumps(report.to_dict(), indent=2, sort_keys=True, default=str))
    return 0 if report.category == "OK" else 1


def _deploy(args: argparse.Namespace) -> int:
    from kimball.common.spark_session import get_spark
    from kimball.ops.deploy import deploy as deploy_fn
    from kimball.ops.runtime_profile import detect_runtime_profile
    from kimball.ops.spark_adapters import build_providers

    target = load_target(args.target, args.targets)
    project = load_compiled_project(args.config, target)
    current = build_manifest(project)
    previous = (
        json.loads(Path(args.against).read_text(encoding="utf-8"))
        if args.against
        else {"pipelines": []}
    )
    targets = list(project.nodes)
    sources = [
        (src.name, src.cdc_strategy == "cdf")
        for node in project.nodes.values()
        for src in node.config.sources
    ]
    secret_refs = [
        col.secret_ref
        for node in project.nodes.values()
        if node.config.pii
        for col in node.config.pii.columns
        if col.secret_ref
    ]
    from kimball.common.secrets import SecretResolver

    resolver = SecretResolver.for_runtime()
    spark = get_spark()
    runtime = detect_runtime_profile(spark)
    providers = build_providers(spark, target.etl_schema)
    result = deploy_fn(
        previous,
        current,
        providers,
        runtime,
        targets,
        sources,
        allow_breaking=args.allow_breaking,
        secret_refs=tuple(secret_refs),
        secret_resolver=resolver,
    )
    print(json.dumps(result.to_dict(), indent=2, sort_keys=True, default=str))
    return 2 if result.blocked else 0


def _query_history(args: argparse.Namespace) -> int:
    # tools/ lives at the repo root, not inside the kimball package.
    _repo = Path(__file__).resolve().parent.parent.parent
    if str(_repo) not in sys.path:
        sys.path.insert(0, str(_repo))
    from tools.query_history import run

    return run(
        days=args.days,
        warehouse=args.warehouse,
        json_output=args.json,
        verbose=args.verbose,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        handler = getattr(args, "handler", None)
        if handler is not None:
            return int(handler(args))
    except (OSError, ValueError, JsonSchemaValidationError, KimballError) as exc:
        from kimball.ops.errors import format_error

        print(format_error(exc), file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
