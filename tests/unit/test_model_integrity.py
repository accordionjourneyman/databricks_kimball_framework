"""Tests for ADR-003 cross-table model-integrity checks.

These tests are spark-free by design and must run on a JVM-less host
(the zero-Spark acceptance test, ADR-003 Verification plan).
"""

from __future__ import annotations

from typing import cast

import pytest
from pydantic import ValidationError

from kimball.common.config import (
    ForeignKeyConfig,
    ForeignKeyLookupConfig,
    SourceConfig,
    SourceContractConfig,
    TableConfig,
)
from kimball.planning.compiler import ProjectCompiler, ProjectValidationError
from kimball.planning.model_integrity import (
    check_project,
)

_UNSET = object()


def _source(
    name: str = "silver.orders",
    primary_keys: list[str] | None | object = _UNSET,
    contract: SourceContractConfig | None = None,
) -> SourceConfig:
    """Defaults to a healthy CDF source with dedup keys.

    Pass ``primary_keys=None`` explicitly for the fixture to express a
    key-less CDF source (the fragile case the INCREMENTAL_LOAD_FRAGILE rule
    flags).
    """
    resolved = ["id"] if primary_keys is _UNSET else primary_keys
    return SourceConfig(
        name=name,
        alias="src",
        primary_keys=cast("list[str] | None", resolved),
        contract=contract,
    )


def _contract(schema: dict[str, str]) -> SourceContractConfig:
    return cast(
        "SourceContractConfig",
        {
            "id": "c1",
            "version": "1.0.0",
            "schema": {name: {"type": t} for name, t in schema.items()},
        },
    )


def _dimension(
    name: str = "gold.dim_customer",
    *,
    column_descriptions: dict[str, str] | None = None,
    **kwargs,
) -> TableConfig:
    sk = kwargs.pop("surrogate_key", "customer_sk")
    return TableConfig(
        table_name=name,
        table_type="dimension",
        surrogate_key=sk,
        natural_keys=kwargs.pop("natural_keys", ["customer_id"]),
        sources=kwargs.pop("sources", [_source(f"silver.{name.split('.')[-1]}")]),
        table_description=kwargs.pop(
            "table_description", f"{name} dimension with governed descriptions."
        ),
        column_descriptions=column_descriptions
        if column_descriptions is not None
        else {sk: "warehouse surrogate key."},
        **kwargs,
    )


def _fact(
    name: str = "gold.fact_sales",
    *,
    foreign_keys=None,
    **kwargs,
) -> TableConfig:
    return TableConfig(
        table_name=name,
        table_type="fact",
        merge_keys=kwargs.pop("merge_keys", ["order_id"]),
        sources=kwargs.pop(
            "sources",
            [
                _source(
                    "silver.orders",
                    primary_keys=["order_id", "customer_id"],
                )
            ],
        ),
        table_description=kwargs.pop(
            "table_description", f"{name} fact with governed descriptions."
        ),
        column_descriptions=kwargs.pop(
            "column_descriptions", {"order_id": "business order identifier."}
        ),
        foreign_keys=foreign_keys,
        **kwargs,
    )


class TestColumnSemanticsConflict:
    def test_clean_project_produces_no_semantics_issues(self) -> None:
        dim = _dimension(
            column_descriptions={"customer_name": "legal name of the customer"}
        )
        fact = _fact(
            column_descriptions={"amount": "order total in USD"},
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        assert check_project({"gold.dim_customer": dim, "gold.fact_sales": fact}) == []

    def test_description_drift_warns(self) -> None:
        dim = _dimension(column_descriptions={"customer_name": "legal name"})
        other = _dimension(
            "gold.dim_supplier", column_descriptions={"customer_name": "plant id"}
        )
        issues = check_project({"gold.dim_customer": dim, "gold.dim_supplier": other})
        codes = [i.code for i in issues]
        assert codes == ["COLUMN_SEMANTICS_CONFLICT"]
        assert issues[0].severity == "warning"
        assert issues[0].fixability == "decision_required"
        assert issues[0].fix is None

    def test_contract_type_clash_errors(self) -> None:
        dim = _dimension(
            sources=[
                _source(
                    "silver.customers",
                    contract=_contract({"customer_address": "string"}),
                )
            ]
        )
        fact = _fact(
            sources=[
                _source(
                    "silver.orders",
                    contract=_contract({"order_id": "long", "order_at": "timestamp"}),
                ),
                _source(
                    "silver.shipments",
                    contract=_contract({"customer_address": "long"}),
                ),
            ],
        )
        issues = check_project({"gold.dim_customer": dim, "gold.fact_sales": fact})
        clashes = [i for i in issues if i.code == "COLUMN_SEMANTICS_CONFLICT"]
        assert all(i.severity == "error" for i in clashes)
        assert any("customer_address" in i.message for i in clashes)

    def test_reserved_columns_ignored(self) -> None:
        dim = _dimension(
            "gold.dim_a",
            column_descriptions={"customer_sk": "surrogate"},
        )
        other = _dimension(
            "gold.dim_b",
            column_descriptions={"customer_sk": "surrogate for b"},
        )
        assert check_project({"gold.dim_a": dim, "gold.dim_b": other}) == []


class TestFactDimensionAttribute:
    def test_clean_project_produces_no_dim_attribute_issues(self) -> None:
        dim = _dimension(
            column_descriptions={"customer_name": "legal name"},
        )
        fact = _fact(
            column_descriptions={"amount": "order total in USD"},
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        assert check_project({"gold.dim_customer": dim, "gold.fact_sales": fact}) == []

    def test_descriptive_attribute_on_fact_warns(self) -> None:
        dim = _dimension(column_descriptions={"customer_name": "legal name"})
        fact = _fact(
            column_descriptions={"customer_name": "copied name"},
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        issues = check_project({"gold.dim_customer": dim, "gold.fact_sales": fact})
        # The drift rule also flags the differing descriptions for the same
        # column; filter to the rule under test.
        attrs = [i for i in issues if i.code == "FACT_DIMENSION_ATTRIBUTE"]
        assert len(attrs) == 1
        assert attrs[0].severity == "warning"
        assert attrs[0].fixability == "decision_required"

    def test_degenerate_dimension_is_allowed(self) -> None:
        dim = _dimension(column_descriptions={"order_number": "business id"})
        fact = _fact(
            degenerate_dimensions=["order_number"],
            column_descriptions={"order_number": "business id"},
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        assert check_project({"gold.dim_customer": dim, "gold.fact_sales": fact}) == []

    def test_exception_suppresses(self) -> None:
        # Per ADR-003 §Decision 4 the engine reports; the compiler suppresses.
        dim = _dimension(
            column_descriptions={
                "customer_name": "legal name",
                "nickname": "informal short name",
            }
        )
        fact = _fact(
            depends_on=["gold.dim_customer"],
            column_descriptions={
                "customer_name": "legal name",
                "nickname": "informal short name",
            },
            modeling_exceptions=[
                {
                    "code": "FACT_DIMENSION_ATTRIBUTE",
                    "columns": ["nickname"],
                    "reason": "dashboard latency",
                }
            ],
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        raw = check_project({"gold.dim_customer": dim, "gold.fact_sales": fact})
        # The engine flags both dim attributes denormalized onto the fact...
        assert {i.column for i in raw if i.code == "FACT_DIMENSION_ATTRIBUTE"} == {
            "customer_name",
            "nickname",
        }
        # ...and the compiler waives exactly the excepted one: 'nickname'
        # disappears from the issues entirely, 'customer_name' stays.
        project = ProjectCompiler(profile="dev").compile(
            [("dim.yml", dim), ("fact.yml", fact)]
        )
        fact_attr_issues = [
            issue
            for issue in project.issues
            if issue.code == "FACT_DIMENSION_ATTRIBUTE"
            and issue.pipeline == "gold.fact_sales"
        ]
        assert len(fact_attr_issues) == 1
        assert "'customer_name'" in fact_attr_issues[0].message


class TestGrainKeyMismatch:
    def test_fk_to_declared_sk_is_clean(self) -> None:
        dim = _dimension()
        fact = _fact(
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ]
        )
        assert check_project({"gold.dim_customer": dim, "gold.fact_sales": fact}) == []

    def test_fk_to_wrong_key_auto_fixes_to_dim_surrogate(self) -> None:
        dim = _dimension()
        fact = _fact(
            foreign_keys=[
                ForeignKeyConfig(
                    column="wrong_sk",
                    references="gold.dim_customer",
                    dimension_key="something_else",
                )
            ]
        )
        issues = check_project({"gold.dim_customer": dim, "gold.fact_sales": fact})
        grain = [i for i in issues if i.code == "GRAIN_KEY_MISMATCH"]
        assert len(grain) == 1
        assert grain[0].fixability == "auto_fixable"
        assert grain[0].fix is not None
        assert grain[0].fix.new == "customer_sk"
        assert grain[0].fix.field == "foreign_keys[wrong_sk].dimension_key"

    def test_type7_fk_fixes_to_durable_key(self) -> None:
        dim = _dimension(
            "gold.dim_customer",
            scd_type=7,
            durable_key="customer_dk",
            effective_at="updated_at",
        )
        fact = _fact(
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_dk",
                    relationship="type7",
                    durable_column="customer_dk",
                    durable_dimension_key="customer_dk",
                    lookup=ForeignKeyLookupConfig(
                        source_columns=["customer_id"],
                        event_time="order_at",
                    ),
                )
            ]
        )
        assert check_project({"gold.dim_customer": dim, "gold.fact_sales": fact}) == []

    def test_natural_key_fk_allowed(self) -> None:
        dim = _dimension()
        fact = _fact(
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_id",
                    references="gold.dim_customer",
                    dimension_key="customer_id",
                )
            ]
        )
        assert check_project({"gold.dim_customer": dim, "gold.fact_sales": fact}) == []


class TestIncrementalLoadFragile:
    def test_cdf_without_primary_keys_suggests_fix(self) -> None:
        fact = _fact(
            sources=[
                _source(
                    "silver.orders",
                    primary_keys=None,
                    contract=_contract({"order_id": "long", "amount": "double"}),
                )
            ]
        )
        issues = check_project({"gold.fact_sales": fact})
        fragile = [i for i in issues if i.code == "INCREMENTAL_LOAD_FRAGILE"]
        assert len(fragile) == 1
        assert fragile[0].fixability == "suggest_fix"
        assert fragile[0].fix is not None
        assert fragile[0].fix.candidates == ("amount", "order_id")

    def test_cdf_with_keys_is_clean(self) -> None:
        fact = _fact()
        assert check_project({"gold.fact_sales": fact}) == []

    def test_hard_delete_on_referenced_table_requires_decision(self) -> None:
        dim = _dimension(delete_strategy="hard")
        fact = _fact(
            depends_on=["gold.dim_customer"],
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        issues = check_project({"gold.dim_customer": dim, "gold.fact_sales": fact})
        assert [i.code for i in issues] == ["INCREMENTAL_LOAD_FRAGILE"]
        assert issues[0].fixability == "decision_required"

    def test_hard_delete_on_leaf_table_is_clean(self) -> None:
        dim = _dimension(delete_strategy="hard")
        assert check_project({"gold.dim_customer": dim}) == []


class TestReferenceCompleteness:
    def test_fk_to_existing_dimension_is_clean(self) -> None:
        dim = _dimension()
        fact = _fact(
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ]
        )
        assert check_project({"gold.dim_customer": dim, "gold.fact_sales": fact}) == []

    def test_reference_to_fact_is_error(self) -> None:
        upstream_fact = _fact("gold.fact_upstream")
        fact = _fact(
            foreign_keys=[
                ForeignKeyConfig(
                    column="upstream_sk",
                    references="gold.fact_upstream",
                    dimension_key="order_id",
                )
            ]
        )
        issues = check_project(
            {"gold.fact_upstream": upstream_fact, "gold.fact_sales": fact}
        )
        assert [i.code for i in issues] == ["MISSING_REFERENCE_TARGET"]
        assert issues[0].severity == "error"
        assert issues[0].fixability == "decision_required"

    def test_missing_reference_with_typo_auto_fixes(self) -> None:
        # 'gold.dim_customera' is a typo of the real dimension name.
        dim = _dimension()
        typo_fact = _fact(
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customerx",
                    dimension_key="customer_sk",
                )
            ]
        )
        issues = check_project(
            {
                "gold.dim_customer": dim,
                "gold.fact_sales": typo_fact,
            }
        )
        missing = [i for i in issues if i.code == "MISSING_REFERENCE_TARGET"]
        assert len(missing) == 1
        assert missing[0].fixability == "auto_fixable"
        assert missing[0].fix is not None
        assert missing[0].fix.new == "gold.dim_customer"

    def test_missing_reference_no_candidates_suggests(self) -> None:
        fact = _fact(
            foreign_keys=[
                ForeignKeyConfig(
                    column="x_sk",
                    references="gold.dim_nowhere",
                    dimension_key="x_sk",
                )
            ]
        )
        issues = check_project({"gold.fact_sales": fact})
        assert [i.code for i in issues] == ["MISSING_REFERENCE_TARGET"]
        assert issues[0].fixability == "suggest_fix"

    def test_orphan_dimension_key_auto_fixes(self) -> None:
        dim = _dimension()
        fact = _fact(
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="not_declared_sk",
                )
            ]
        )
        issues = check_project({"gold.dim_customer": dim, "gold.fact_sales": fact})
        orphan = [i for i in issues if i.code == "ORPHAN_REFERENCE"]
        assert len(orphan) == 1
        assert orphan[0].fixability == "auto_fixable"
        assert orphan[0].fix is not None
        assert orphan[0].fix.new == "customer_sk"


class TestModelingExceptionConfig:
    def test_unknown_code_rejected(self) -> None:
        with pytest.raises(ValidationError, match="unknown modeling-exception code"):
            _dimension(
                modeling_exceptions=[{"code": "NOPE", "columns": ["a"], "reason": "r"}]
            )

    def test_blank_reason_rejected(self) -> None:
        with pytest.raises(ValidationError, match="reason"):
            _dimension(
                modeling_exceptions=[
                    {"code": "GRAIN_KEY_MISMATCH", "columns": ["a"], "reason": "  "}
                ]
            )

    def test_empty_columns_rejected(self) -> None:
        with pytest.raises(ValidationError, match="columns"):
            _dimension(
                modeling_exceptions=[
                    {"code": "GRAIN_KEY_MISMATCH", "columns": [], "reason": "r"}
                ]
            )

    def test_duplicate_code_column_pair_rejected(self) -> None:
        with pytest.raises(ValidationError, match="must not repeat"):
            _dimension(
                modeling_exceptions=[
                    {"code": "ORPHAN_REFERENCE", "columns": ["a"], "reason": "r1"},
                    {"code": "ORPHAN_REFERENCE", "columns": ["a"], "reason": "r2"},
                ]
            )


class TestCompilerIntegration:
    def test_production_promotes_warning_to_error(self) -> None:
        dim = _dimension(column_descriptions={"customer_name": "legal name"})
        fact = _fact(
            column_descriptions={"customer_name": "copied"},
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        with pytest.raises(ProjectValidationError, match="FACT_DIMENSION_ATTRIBUTE"):
            ProjectCompiler(profile="production").compile(
                [("dim.yml", dim), ("fact.yml", fact)]
            )

    def test_dev_profile_keeps_it_a_warning(self) -> None:
        dim = _dimension(column_descriptions={"customer_name": "legal name"})
        fact = _fact(
            column_descriptions={"customer_name": "copied"},
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        project = ProjectCompiler(profile="dev").compile(
            [("dim.yml", dim), ("fact.yml", fact)]
        )
        assert [
            issue.code
            for issue in project.warnings
            if issue.code == "FACT_DIMENSION_ATTRIBUTE"
        ]

    def test_exception_suppresses_in_production_and_emits_approved(self) -> None:
        dim = _dimension(column_descriptions={"customer_name": "legal name"})
        fact = _fact(
            depends_on=["gold.dim_customer"],
            column_descriptions={"customer_name": "copied"},
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
            modeling_exceptions=[
                {
                    "code": "COLUMN_SEMANTICS_CONFLICT",
                    "columns": ["customer_name"],
                    "reason": "name semantics intentionally narrower on fact",
                },
                {
                    "code": "FACT_DIMENSION_ATTRIBUTE",
                    "columns": ["customer_name"],
                    "reason": "dashboard latency spike, see ADR-002",
                    "decision_ref": "phase2/evidence/ADR-002",
                },
            ],
        )
        project = ProjectCompiler(profile="production").compile(
            [("dim.yml", dim), ("fact.yml", fact)]
        )
        approved = [i for i in project.issues if i.code == "EXCEPTION_APPROVED"]
        # Both findings on customer_name are waived (drift + dim-attribute);
        # only the dim-attribute ledger entry carries a decision_ref.
        assert len(approved) == 2
        assert any(
            "decision_ref=phase2/evidence/ADR-002" in issue.message
            for issue in approved
        )
        assert project.model_integrity_summary is not None
        assert "2 exception-approved" in project.model_integrity_summary

    def test_mismatched_column_does_not_suppress(self) -> None:
        dim = _dimension(column_descriptions={"customer_name": "legal name"})
        fact = _fact(
            column_descriptions={"customer_name": "copied"},
            modeling_exceptions=[
                {
                    "code": "FACT_DIMENSION_ATTRIBUTE",
                    "columns": ["other_column"],
                    "reason": "unrelated",
                }
            ],
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        with pytest.raises(ProjectValidationError, match="FACT_DIMENSION_ATTRIBUTE"):
            ProjectCompiler(profile="production").compile(
                [("dim.yml", dim), ("fact.yml", fact)]
            )

    def test_strict_promotes_to_error_in_dev(self) -> None:
        dim = _dimension(column_descriptions={"customer_name": "legal name"})
        fact = _fact(
            column_descriptions={"customer_name": "copied"},
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        with pytest.raises(ProjectValidationError, match="FACT_DIMENSION_ATTRIBUTE"):
            ProjectCompiler(profile="dev", strict=True).compile(
                [("dim.yml", dim), ("fact.yml", fact)]
            )

    def test_summary_line_shape(self) -> None:
        dim = _dimension(column_descriptions={"customer_name": "legal name"})
        fact = _fact(
            column_descriptions={
                "amount": "order total in USD",
                "customer_name": "copied",
            },
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        project = ProjectCompiler(profile="dev").compile(
            [("dim.yml", dim), ("fact.yml", fact)]
        )
        assert project.model_integrity_summary is not None
        assert project.model_integrity_summary.startswith("model-integrity:")
        # Both the drift check and the dim-attribute check fire for the same
        # reused column; the summary must count both.
        assert "2 warning" in project.model_integrity_summary
        assert "0 error" in project.model_integrity_summary


def test_empty_project_is_clean() -> None:
    project = ProjectCompiler().compile([])
    assert project.model_integrity_summary == "model-integrity: no issues"


class TestRulePolicy:
    """ADR-003 §Decision 8: per-target rule enable/disable/customize."""

    def test_disabled_rule_skips_findings_and_emits_rule_disabled(self) -> None:
        from kimball.common.config import (
            ModelIntegrityPolicy,
            ModelIntegrityRulePolicy,
        )

        dim = _dimension(column_descriptions={"customer_name": "legal name"})
        fact = _fact(
            depends_on=["gold.dim_customer"],
            column_descriptions={"customer_name": "legal name"},
            foreign_keys=[
                ForeignKeyConfig(
                    column="customer_sk",
                    references="gold.dim_customer",
                    dimension_key="customer_sk",
                )
            ],
        )
        policy = ModelIntegrityPolicy(
            rules=[
                ModelIntegrityRulePolicy(code="FACT_DIMENSION_ATTRIBUTE", enabled=False)
            ]
        )
        project = ProjectCompiler(profile="production", rule_policy=policy).compile(
            [("dim.yml", dim), ("fact.yml", fact)]
        )
        codes = [issue.code for issue in project.issues]
        assert "FACT_DIMENSION_ATTRIBUTE" not in codes
        assert "RULE_DISABLED" in codes
        assert "model-integrity:" in (project.model_integrity_summary or "")
        assert "rule-disabled" in (project.model_integrity_summary or "")

    def test_severity_override_downgrades_in_production(self) -> None:
        from kimball.common.config import (
            ModelIntegrityPolicy,
            ModelIntegrityRulePolicy,
        )

        # MISSING_DESCRIPTION is engine-warning; override to error and verify
        # production compile fails on it. Use the inverse: a rule error via
        # severity=error in dev profile compiles cleanly (warnings only).
        dim = _dimension()
        policy = ModelIntegrityPolicy(
            rules=[
                ModelIntegrityRulePolicy(code="GRAIN_KEY_MISMATCH", severity="warning"),
                ModelIntegrityRulePolicy(code="ORPHAN_REFERENCE", enabled=False),
            ]
        )
        # A GRAIN_KEY_MISMATCH finding in production is promoted to error by
        # the profile; the policy override keeps it a warning while the
        # interfering ORPHAN_REFERENCE is disabled by policy.
        fact_mismatch = _fact(
            depends_on=["gold.dim_customer"],
            foreign_keys=[
                ForeignKeyConfig(
                    column="wrong_sk",
                    references="gold.dim_customer",
                    dimension_key="not_declared",
                )
            ],
        )
        project = ProjectCompiler(profile="production", rule_policy=policy).compile(
            [("dim.yml", dim), ("fact.yml", fact_mismatch)]
        )
        grain = [
            issue for issue in project.issues if issue.code == "GRAIN_KEY_MISMATCH"
        ]
        assert len(grain) == 1
        assert grain[0].severity == "warning"

    def test_profile_override_applies_when_no_policy_severity(self) -> None:
        from kimball.common.config import ModelIntegrityPolicy

        dim = _dimension()
        fact_mismatch = _fact(
            depends_on=["gold.dim_customer"],
            foreign_keys=[
                ForeignKeyConfig(
                    column="wrong_sk",
                    references="gold.dim_customer",
                    dimension_key="not_declared",
                )
            ],
        )
        # Empty policy: production still error-promotes the warning finding.
        with pytest.raises(ProjectValidationError, match="GRAIN_KEY_MISMATCH"):
            ProjectCompiler(
                profile="production", rule_policy=ModelIntegrityPolicy()
            ).compile([("dim.yml", dim), ("fact.yml", fact_mismatch)])

    def test_param_reaches_rule(self) -> None:

        fact = _fact(sources=[_source("silver.orders", primary_keys=None)])
        # Default: key-less CDF source flagged...
        assert any(
            i.code == "INCREMENTAL_LOAD_FRAGILE"
            for i in check_project({"gold.fact_sales": fact})
        )
        # ...unless require_primary_keys=False.
        from kimball.planning.model_integrity import check_project as cp

        findings = cp(
            {"gold.fact_sales": fact},
            rule_params={"INCREMENTAL_LOAD_FRAGILE": {"require_primary_keys": False}},
        )
        assert all(i.code != "INCREMENTAL_LOAD_FRAGILE" for i in findings)

    def test_unknown_param_rejected_at_config_load(self) -> None:
        from kimball.common.config import (
            ModelIntegrityPolicy,
            ModelIntegrityRulePolicy,
        )

        with pytest.raises(ValidationError, match="does not accept parameter"):
            ModelIntegrityPolicy(
                rules=[
                    ModelIntegrityRulePolicy(
                        code="MISSING_DESCRIPTION",
                        params={"require_primary_keys": True},
                    )
                ]
            )

    def test_unknown_code_rejected_in_policy(self) -> None:
        from kimball.common.config import (
            ModelIntegrityPolicy,
            ModelIntegrityRulePolicy,
        )

        with pytest.raises(ValidationError, match="unknown model_integrity rule"):
            ModelIntegrityPolicy(rules=[ModelIntegrityRulePolicy(code="NO_SUCH_RULE")])

    def test_rule_disabled_counted_in_summary(self) -> None:
        from kimball.common.config import (
            ModelIntegrityPolicy,
            ModelIntegrityRulePolicy,
        )

        dim = _dimension()
        fact = _fact(
            depends_on=["gold.dim_customer"],
            sources=[_source("silver.orders", primary_keys=None)],
        )
        policy = ModelIntegrityPolicy(
            rules=[
                ModelIntegrityRulePolicy(code="INCREMENTAL_LOAD_FRAGILE", enabled=False)
            ]
        )
        project = ProjectCompiler(profile="dev", rule_policy=policy).compile(
            [("dim.yml", dim), ("fact.yml", fact)]
        )
        disabled = [issue for issue in project.issues if issue.code == "RULE_DISABLED"]
        assert disabled
        assert "rule-disabled" in (project.model_integrity_summary or "")
