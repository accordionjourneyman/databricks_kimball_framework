from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from kimball.common.errors import DataQualityError
from kimball.orchestration.services.skeleton_manager import (
    SkeletonGenerationResult,
    SkeletonManager,
)


def _ctx(action: str = "skeleton") -> MagicMock:
    source = SimpleNamespace(
        name="silver.orders",
        alias="orders",
        contract=None,
    )
    ctx = MagicMock()
    ctx.config.early_arriving_facts = []
    ctx.config.early_arriving_dimensions = [
        SimpleNamespace(
            fact_key="customer_id",
            dimension_table="gold.dim_customer",
            dimension_key="customer_id",
            surrogate_key="customer_sk",
            action=action,
        )
    ]
    ctx.config.sources = [source]
    ctx.config.table_name = "gold.fact_orders"
    ctx.config.effective_at = None
    ctx.config.observability = None
    ctx.etl_control.schema = "ops"
    ctx.batch_id = "run-42"
    return ctx


def _result() -> SkeletonGenerationResult:
    return SkeletonGenerationResult(
        dimension_table="gold.dim_customer",
        missing_rows=2,
        samples=[{"customer_id": 99}],
        status="created",
    )


def test_skeleton_creation_writes_durable_early_fact_finding() -> None:
    generator = MagicMock()
    generator.generate_skeletons.return_value = _result()
    manager = SkeletonManager(generator)
    ctx = _ctx()
    orders = MagicMock()
    orders.columns = ["order_id", "customer_id"]

    with patch(
        "kimball.orchestration.services.skeleton_manager.DataQualityEventSink"
    ) as sink_type:
        manager.generate_skeletons(ctx, {"silver.orders": orders})

    generator.generate_skeletons.assert_called_once_with(
        fact_df=orders,
        dim_table_name="gold.dim_customer",
        fact_join_key="customer_id",
        dim_join_key="customer_id",
        surrogate_key_col="customer_sk",
        batch_id="run-42",
        effective_at_column=None,
        create=True,
    )
    written = sink_type.return_value.write.call_args.kwargs
    assert written["action"] == "skeleton_created"
    assert written["finding"].category == "early_arriving_fact"
    assert written["finding"].failed_rows == 2


def test_error_policy_blocks_before_creating_skeletons() -> None:
    generator = MagicMock()
    generator.generate_skeletons.return_value = _result()
    manager = SkeletonManager(generator)
    ctx = _ctx(action="error")
    orders = MagicMock()
    orders.columns = ["customer_id"]

    with (
        patch(
            "kimball.orchestration.services.skeleton_manager.DataQualityEventSink"
        ) as sink_type,
        pytest.raises(DataQualityError, match="early-arriving"),
    ):
        manager.generate_skeletons(ctx, {"silver.orders": orders})

    assert generator.generate_skeletons.call_args.kwargs["create"] is False
    assert sink_type.return_value.write.call_args.kwargs["action"] == "blocked"
