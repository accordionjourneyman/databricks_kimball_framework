from unittest.mock import MagicMock, patch

import pytest

from kimball.common.errors import DataQualityError
from kimball.processing.key_integrity import validate_type7_keys


def _key_frame() -> MagicMock:
    frame = MagicMock()
    frame.columns = [
        "customer_sk",
        "customer_dk",
        "__row_key_fingerprint",
        "__durable_key_fingerprint",
    ]
    return frame


def test_type7_integrity_gate_aggregates_both_key_domains_once() -> None:
    source = _key_frame()
    target = MagicMock()
    target.columns = []
    row_domain = MagicMock()
    durable_domain = MagicMock()
    combined = MagicMock()
    source.select.side_effect = [row_domain, durable_domain]
    row_domain.unionByName.return_value = combined
    violations = combined.filter.return_value.groupBy.return_value.agg.return_value.filter.return_value.limit.return_value
    violations.collect.return_value = []

    with patch("kimball.processing.key_integrity.F") as functions:
        functions.col.return_value.__gt__.return_value = MagicMock()
        functions.col.return_value.__eq__.return_value = MagicMock()
        validate_type7_keys(
            source,
            target,
            surrogate_key="customer_sk",
            durable_key="customer_dk",
        )

    violations.collect.assert_called_once()


def test_type7_integrity_gate_reports_collision() -> None:
    source = _key_frame()
    target = MagicMock()
    target.columns = []
    row_domain = MagicMock()
    durable_domain = MagicMock()
    combined = MagicMock()
    source.select.side_effect = [row_domain, durable_domain]
    row_domain.unionByName.return_value = combined
    violations = combined.filter.return_value.groupBy.return_value.agg.return_value.filter.return_value.limit.return_value
    violations.collect.return_value = [
        {"__key_kind": "row", "__key": 42, "__payloads": 2}
    ]

    with patch("kimball.processing.key_integrity.F") as functions:
        functions.col.return_value.__gt__.return_value = MagicMock()
        functions.col.return_value.__eq__.return_value = MagicMock()
        expected_error = pytest.raises(DataQualityError, match="BIGINT 42")
        with expected_error:
            validate_type7_keys(
                source,
                target,
                surrogate_key="customer_sk",
                durable_key="customer_dk",
            )


def test_type7_integrity_gate_requires_fingerprints() -> None:
    source = MagicMock()
    source.columns = ["customer_sk", "customer_dk"]
    target = MagicMock()
    target.columns = []

    with pytest.raises(DataQualityError, match="fingerprints are missing"):
        validate_type7_keys(
            source,
            target,
            surrogate_key="customer_sk",
            durable_key="customer_dk",
        )
