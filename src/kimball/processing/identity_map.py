from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from pyspark.sql import DataFrame, SparkSession


class IdentityMapError(ValueError):
    """Raised when supplied survivorship decisions are temporally ambiguous."""


IDENTITY_MAP_COLUMNS = (
    "source_identity",
    "canonical_identity",
    "valid_from",
    "valid_to",
)


def validate_identity_rows(
    rows: Sequence[Mapping[str, Any]], *, max_chain_depth: int = 32
) -> None:
    """Validate a bounded set of governed identity decisions.

    This validates control-plane decisions before they are materialized. The
    framework does not infer or choose a survivor.
    """

    by_source: dict[Any, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        required = (
            row.get("source_identity"),
            row.get("canonical_identity"),
            row.get("valid_from"),
            row.get("valid_to"),
        )
        if any(value is None for value in required):
            raise IdentityMapError("identity map fields must be non-null")
        if row["valid_from"] >= row["valid_to"]:
            raise IdentityMapError("identity map intervals must have positive duration")
        by_source[row["source_identity"]].append(row)

    for source, versions in by_source.items():
        ordered = sorted(versions, key=lambda row: row["valid_from"])
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if current["valid_from"] < previous["valid_to"]:
                raise IdentityMapError(
                    f"identity map overlap for source identity {source!r}"
                )

    # Detect cycles in each interval where an edge is active. A chain ending in
    # a self-map is a valid terminal survivor; revisiting another node is not.
    boundaries = sorted(
        {row["valid_from"] for row in rows} | {row["valid_to"] for row in rows}
    )
    for instant in boundaries[:-1]:
        active = {
            row["source_identity"]: row["canonical_identity"]
            for row in rows
            if row["valid_from"] <= instant < row["valid_to"]
        }
        for start in active:
            seen: set[Any] = set()
            current = start
            for _ in range(max_chain_depth + 1):
                target = active.get(current, current)
                if target == current:
                    break
                if current in seen or target in seen:
                    raise IdentityMapError(f"identity map cycle involving {start!r}")
                seen.add(current)
                current = target
            else:
                raise IdentityMapError(
                    f"identity map chain exceeds max depth {max_chain_depth}"
                )


def load_validated_identity_map(
    spark: SparkSession,
    table_name: str,
    *,
    max_control_rows: int = 100_000,
) -> DataFrame:
    """Load a governed temporal identity map and fail closed before joining it."""
    frame = spark.table(table_name)
    missing = sorted(set(IDENTITY_MAP_COLUMNS) - set(frame.columns))
    if missing:
        raise IdentityMapError(
            f"identity map {table_name} is missing: {', '.join(missing)}"
        )
    selected = frame.select(*IDENTITY_MAP_COLUMNS)
    rows = selected.limit(max_control_rows + 1).collect()
    if len(rows) > max_control_rows:
        raise IdentityMapError(
            f"identity map {table_name} exceeds the {max_control_rows}-row "
            "control-plane validation budget"
        )
    payload = [row.asDict(recursive=True) for row in rows]
    validate_identity_rows(payload)

    # Runtime resolution intentionally supports direct survivor mappings. A
    # producer must flatten chains so every source points at its final survivor.
    for left in payload:
        for right in payload:
            overlaps = (
                left["valid_from"] < right["valid_to"]
                and right["valid_from"] < left["valid_to"]
            )
            if (
                overlaps
                and left["source_identity"] != left["canonical_identity"]
                and left["canonical_identity"] == right["source_identity"]
                and right["canonical_identity"] != right["source_identity"]
            ):
                raise IdentityMapError(
                    "identity map chains must be flattened to the final survivor"
                )
    return selected
