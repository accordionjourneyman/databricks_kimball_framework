from __future__ import annotations

from bisect import bisect_left, bisect_right
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


def _has_unflattened_chain(rows: Sequence[Mapping[str, Any]]) -> bool:
    """Return whether an overlapping mapping points at another nonterminal edge.

    Call after ``validate_identity_rows``: source timelines then have positive,
    non-overlapping intervals, which lets each target timeline answer overlap
    queries with two binary searches and a prefix count.
    """
    by_source: dict[Any, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[row["source_identity"]].append(row)

    indexes: dict[Any, tuple[list[Any], list[Any], list[int]]] = {}
    for source, versions in by_source.items():
        ordered = sorted(versions, key=lambda row: row["valid_from"])
        starts = [row["valid_from"] for row in ordered]
        ends = [row["valid_to"] for row in ordered]
        nonterminal_prefix = [0]
        for row in ordered:
            nonterminal_prefix.append(
                nonterminal_prefix[-1] + int(row["canonical_identity"] != source)
            )
        indexes[source] = starts, ends, nonterminal_prefix

    for row in rows:
        source = row["source_identity"]
        target = row["canonical_identity"]
        if source == target or target not in indexes:
            continue
        starts, ends, nonterminal_prefix = indexes[target]
        first_overlap = bisect_right(ends, row["valid_from"])
        end_overlap = bisect_left(starts, row["valid_to"])
        if nonterminal_prefix[end_overlap] > nonterminal_prefix[first_overlap]:
            return True
    return False


def load_validated_identity_map(
    spark: SparkSession,
    table_name: str,
    *,
    max_control_rows: int = 100_000,
    version_as_of: int | None = None,
) -> DataFrame:
    """Load a governed temporal identity map and fail closed before joining it."""
    if version_as_of is None:
        frame = spark.table(table_name)
    else:
        frame = (
            spark.read.format("delta")
            .option("versionAsOf", version_as_of)
            .table(table_name)
        )
    if missing := sorted(set(IDENTITY_MAP_COLUMNS) - set(frame.columns)):
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
    if _has_unflattened_chain(payload):
        raise IdentityMapError(
            "identity map chains must be flattened to the final survivor"
        )
    return selected
