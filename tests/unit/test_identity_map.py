from datetime import datetime
from unittest.mock import MagicMock

import pytest

from kimball.processing.identity_map import (
    IDENTITY_MAP_COLUMNS,
    IdentityMapError,
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
    selected = frame.select.return_value
    selected.limit.return_value.collect.return_value = []

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


def test_identity_map_loader_rejects_unflattened_survivor_chain() -> None:
    spark = MagicMock()
    frame = spark.table.return_value
    frame.columns = list(IDENTITY_MAP_COLUMNS)
    selected = frame.select.return_value
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
