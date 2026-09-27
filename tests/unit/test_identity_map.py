import random
from datetime import datetime
from typing import cast
from unittest.mock import MagicMock

import pytest

from kimball.processing.identity_map import (
    IDENTITY_MAP_COLUMNS,
    IdentityMapError,
    _has_unflattened_chain,
    load_validated_identity_map,
    validate_identity_rows,
)


def _row(source: str, canonical: str, start: str, end: str = "9999-12-31"):
    return {
        "source_identity": source,
        "canonical_identity": canonical,
        "valid_from": datetime.fromisoformat(start),
        "valid_to": datetime.fromisoformat(end),
    }


def test_identity_map_accepts_temporal_merge_and_unmerge() -> None:
    validate_identity_rows(
        [
            _row("B", "B", "2024-01-01", "2025-01-01"),
            _row("B", "A", "2025-01-01", "2026-01-01"),
            _row("B", "B", "2026-01-01"),
            _row("A", "A", "2024-01-01"),
        ]
    )


def test_identity_map_rejects_overlapping_survivors() -> None:
    with pytest.raises(IdentityMapError, match="overlap"):
        validate_identity_rows(
            [
                _row("B", "A", "2025-01-01", "2026-01-01"),
                _row("B", "C", "2025-06-01", "2027-01-01"),
            ]
        )


def test_identity_map_rejects_cycles() -> None:
    with pytest.raises(IdentityMapError, match="cycle"):
        validate_identity_rows(
            [
                _row("A", "B", "2025-01-01"),
                _row("B", "A", "2025-01-01"),
            ]
        )


def test_identity_map_rejects_nulls_and_invalid_intervals() -> None:
    with pytest.raises(IdentityMapError, match="non-null"):
        validate_identity_rows(
            [
                {
                    "source_identity": None,
                    "canonical_identity": "A",
                    "valid_from": datetime(2025, 1, 1),
                    "valid_to": datetime(2026, 1, 1),
                }
            ]
        )
    with pytest.raises(IdentityMapError, match="positive"):
        validate_identity_rows([_row("A", "A", "2026-01-01", "2025-01-01")])


def test_identity_map_loader_validates_schema_budget_and_payload() -> None:
    spark = MagicMock()
    frame = MagicMock()
    spark.table.return_value = frame
    frame.columns = list(IDENTITY_MAP_COLUMNS)
    selected = cast("MagicMock", frame.select.return_value)
    cast("MagicMock", selected.limit.return_value).collect.return_value = []

    assert load_validated_identity_map(spark, "silver.identity_map") is selected

    frame.columns = ["source_identity"]
    with pytest.raises(IdentityMapError, match="is missing"):
        load_validated_identity_map(spark, "silver.identity_map")

    frame.columns = list(IDENTITY_MAP_COLUMNS)
    selected.limit.return_value.collect.return_value = [MagicMock(), MagicMock()]
    with pytest.raises(IdentityMapError, match="validation budget"):
        load_validated_identity_map(
            spark,
            "silver.identity_map",
            max_control_rows=1,
        )


def test_identity_map_loader_reads_the_requested_delta_version() -> None:
    spark = MagicMock()
    frame = spark.read.format.return_value.option.return_value.table.return_value
    frame.columns = list(IDENTITY_MAP_COLUMNS)
    selected = cast("MagicMock", frame.select.return_value)
    cast("MagicMock", selected.limit.return_value).collect.return_value = []

    loaded = load_validated_identity_map(
        spark,
        "silver.identity_map",
        version_as_of=42,
    )

    assert loaded is selected
    spark.read.format.assert_called_once_with("delta")
    spark.read.format.return_value.option.assert_called_once_with("versionAsOf", 42)
    spark.table.assert_not_called()


def test_identity_map_loader_rejects_unflattened_survivor_chain() -> None:
    spark = MagicMock()
    frame = cast("MagicMock", spark.table.return_value)
    frame.columns = list(IDENTITY_MAP_COLUMNS)
    selected = cast("MagicMock", frame.select.return_value)
    rows = [
        _row("B", "A", "2025-01-01"),
        _row("A", "C", "2025-01-01"),
        _row("C", "C", "2025-01-01"),
    ]
    selected.limit.return_value.collect.return_value = [
        MagicMock(asDict=MagicMock(return_value=row)) for row in rows
    ]

    with pytest.raises(IdentityMapError, match="flattened"):
        load_validated_identity_map(spark, "silver.identity_map")


def test_identity_map_flattening_uses_half_open_overlap_semantics() -> None:
    touching = [
        _row("A", "B", "2025-01-01", "2025-01-06"),
        _row("B", "C", "2025-01-06", "2025-01-10"),
    ]
    validate_identity_rows(touching)
    assert _has_unflattened_chain(touching) is False

    overlapping = [
        _row("A", "B", "2025-01-01", "2025-01-06"),
        _row("B", "C", "2025-01-05", "2025-01-10"),
    ]
    validate_identity_rows(overlapping)
    assert _has_unflattened_chain(overlapping) is True


def test_identity_map_flattening_finds_nonterminal_target_version_in_span() -> None:
    rows = [
        _row("A", "B", "2025-01-01", "2025-01-10"),
        _row("B", "B", "2025-01-01", "2025-01-05"),
        _row("B", "C", "2025-01-05", "2025-01-10"),
    ]

    validate_identity_rows(rows)
    assert _has_unflattened_chain(rows) is True


def test_identity_map_flattening_accepts_only_terminal_target_versions() -> None:
    rows = [
        _row("A", "B", "2025-01-01", "2025-01-10"),
        _row("B", "B", "2025-01-01", "2025-01-05"),
        _row("B", "B", "2025-01-05", "2025-01-10"),
    ]

    validate_identity_rows(rows)
    assert _has_unflattened_chain(rows) is False


def test_indexed_chain_detector_matches_quadratic_reference_on_seeded_timelines():
    rng = random.Random(20260929)

    def quadratic_reference(rows: list[dict]) -> bool:
        for left in rows:
            for right in rows:
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
                    return True
        return False

    for _ in range(30):
        identities = [f"id_{index:02}" for index in range(12)]
        rows = []
        for index, source in enumerate(identities):
            for version in range(rng.randint(1, 4)):
                canonical = identities[rng.randrange(index, len(identities))]
                rows.append(
                    {
                        "source_identity": source,
                        "canonical_identity": canonical,
                        "valid_from": version * 10,
                        "valid_to": (version + 1) * 10,
                    }
                )

        validate_identity_rows(rows)
        assert _has_unflattened_chain(rows) == quadratic_reference(rows)
