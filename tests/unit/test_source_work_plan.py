from __future__ import annotations

from kimball.common.config import SourceConfig
from kimball.orchestration.services.work_plan import build_source_work_plan


def _source(name: str, strategy: str = 'cdf', starting_version: int = 0):
    return SourceConfig.model_validate(
        {
            'name': name,
            'alias': name.replace('.', '_'),
            'cdc_strategy': strategy,
            'starting_version': starting_version,
        }
    )


def test_plan_skips_caught_up_incremental_sources() -> None:
    plan = build_source_work_plan(
        [_source('silver.orders')],
        watermarks={'silver.orders': 8},
        latest_versions={'silver.orders': 8},
        preserve_all_changes=False,
    )

    assert plan.active_items == ()
    assert plan.items[0].active is False


def test_plan_selects_only_next_version_when_preserving_changes() -> None:
    plan = build_source_work_plan(
        [_source('silver.orders', starting_version=2)],
        watermarks={'silver.orders': 4},
        latest_versions={'silver.orders': 9},
        preserve_all_changes=True,
    )

    item = plan.active_items[0]
    assert (item.starting_version, item.ending_version) == (5, 5)
    assert item.delete_mode == 'explicit_cdf'


def test_plan_marks_full_snapshot_active_without_versions() -> None:
    plan = build_source_work_plan(
        [_source('silver.customers', strategy='full')],
        watermarks={},
        latest_versions={},
        preserve_all_changes=False,
    )

    item = plan.active_items[0]
    assert item.starting_version is None
    assert item.ending_version is None
    assert item.delete_mode == 'full_snapshot'


def test_mixed_sources_disable_table_snapshot_reconciliation() -> None:
    plan = build_source_work_plan(
        [
            _source('silver.customers', strategy='full'),
            _source('silver.events', strategy='cdf'),
        ],
        watermarks={'silver.events': 1},
        latest_versions={'silver.events': 2},
        preserve_all_changes=False,
    )

    assert plan.full_snapshot_reconciliation is False
