from __future__ import annotations

from kimball.common.canonical import canonical_config_payload, canonical_digest
from kimball.common.config import (
    ConfigLoader,
    ObservabilityConfig,
    PIIColumnConfig,
    PIIPolicy,
    RowFilterConfig,
    SourceConfig,
    TableConfig,
)
from kimball.planning.compiler import ProjectCompiler
from kimball.planning.manifest import build_manifest


def _config(**overrides) -> TableConfig:
    values = {
        "table_name": "gold.fact_orders",
        "table_type": "fact",
        "merge_keys": ["order_id"],
        "sources": [
            SourceConfig(
                name="silver.orders",
                alias="orders",
                primary_keys=["order_id"],
                options={"region": "eu"},
            )
        ],
    }
    values.update(overrides)
    return TableConfig.model_validate(values)


def _fingerprint(config: TableConfig) -> str:
    return ConfigLoader(env_vars={}).compute_fingerprint(config)


def test_config_fingerprint_covers_behavior_fields_omitted_by_the_old_selection():
    base = _config()
    changed = [
        _config(
            sources=[base.sources[0].model_copy(update={"options": {"region": "us"}})]
        ),
        _config(append_only=True),
        _config(
            pii=PIIPolicy(
                columns=[
                    PIIColumnConfig(
                        column="email", strategy="tokenize", secret_ref="vault://pii-a"
                    )
                ]
            )
        ),
        _config(
            row_filter=RowFilterConfig(
                function_name="filter_orders",
                function_body="region = 'eu'",
                column="region",
            )
        ),
        _config(observability=ObservabilityConfig(write_failure="error")),
    ]

    baseline = _fingerprint(base)
    assert baseline.startswith("v2:") and len(baseline) == 67
    assert all(_fingerprint(candidate) != baseline for candidate in changed)
    assert (
        _fingerprint(base.model_copy(update={"table_description": "reworded"}))
        == baseline
    )


def test_fingerprint_respects_sql_override_even_when_it_is_empty():
    config = _config(transformation_sql="SELECT * FROM orders")
    loader = ConfigLoader(env_vars={})

    assert loader.compute_fingerprint(
        config, sql_text=""
    ) != loader.compute_fingerprint(config)


def test_manifest_and_run_fingerprint_share_canonical_payload_and_redact_credentials():
    secret = "DO-NOT-EMIT-this-password"
    config = _config(
        sources=[
            SourceConfig(
                name="silver.orders",
                alias="orders",
                primary_keys=["order_id"],
                options={"password": secret, "region": "eu"},
            )
        ]
    )
    project = ProjectCompiler().compile([("orders.yml", config)])
    manifest = build_manifest(project, framework_version="test")
    pipeline = manifest["pipelines"][0]
    canonical_payload = canonical_config_payload(config)

    assert ConfigLoader(env_vars={}).compute_fingerprint(
        config
    ) == "v2:" + canonical_digest(canonical_payload)
    assert pipeline["semantic_config"] == canonical_payload
    assert pipeline["semantic_digest"] == canonical_digest(canonical_payload)
    assert (
        pipeline["semantic_config"]["sources"][0]["options"]["password"] == "<redacted>"
    )
    assert secret not in str(manifest)
    assert manifest["schema_version"] == "1.1"
