from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from kimball.common.config import SourceConfig

DeleteMode = Literal["explicit_cdf", "full_snapshot"]


@dataclass(frozen=True)
class SourceWorkItem:
    source: SourceConfig
    prior_watermark: int | None
    latest_version: int | None
    starting_version: int | None
    ending_version: int | None
    active: bool
    delete_mode: DeleteMode

    @property
    def source_name(self) -> str:
        return self.source.name


@dataclass(frozen=True)
class SourceWorkPlan:
    items: tuple[SourceWorkItem, ...]

    @property
    def active_items(self) -> tuple[SourceWorkItem, ...]:
        return tuple(item for item in self.items if item.active)

    @property
    def full_snapshot_reconciliation(self) -> bool:
        active = self.active_items
        return bool(active) and all(
            item.delete_mode == "full_snapshot" for item in active
        )


def build_source_work_plan(
    sources: Sequence[SourceConfig],
    *,
    watermarks: dict[str, int | None],
    latest_versions: dict[str, int],
    preserve_all_changes: bool,
) -> SourceWorkPlan:
    items: list[SourceWorkItem] = []
    for source in sources:
        if source.cdc_strategy == "full":
            items.append(
                SourceWorkItem(
                    source=source,
                    prior_watermark=None,
                    latest_version=None,
                    starting_version=None,
                    ending_version=None,
                    active=True,
                    delete_mode="full_snapshot",
                )
            )
            continue

        latest = latest_versions[source.name]
        watermark = watermarks.get(source.name)
        start = source.starting_version if watermark is None else watermark + 1
        active = start <= latest
        end = start if active and preserve_all_changes else latest
        items.append(
            SourceWorkItem(
                source=source,
                prior_watermark=watermark,
                latest_version=latest,
                starting_version=start,
                ending_version=end,
                active=active,
                delete_mode="explicit_cdf",
            )
        )
    return SourceWorkPlan(tuple(items))
