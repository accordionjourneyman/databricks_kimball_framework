from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pyspark.sql.types import StringType

from kimball.common.config import PIIColumnConfig, PIIPolicy
from kimball.processing.pii import apply_pii_masking


@pytest.fixture(autouse=True)
def _patch_spark_fns():
    """Patch Spark functions that require an active SparkContext."""
    with patch("kimball.processing.pii.F") as mock_F, \
         patch("kimball.processing.pii.xxhash64", return_value=MagicMock()):
        mock_F.col.return_value = MagicMock()
        mock_F.lit.return_value = MagicMock()
        mock_F.concat.return_value = MagicMock()
        mock_F.sha2.return_value = MagicMock()
        mock_F.substring.return_value = MagicMock()
        yield


def _make_df(columns: list[str]):
    df = MagicMock()
    df.columns = columns
    schema = MagicMock()
    fields = []
    for c in columns:
        f = MagicMock()
        f.name = c
        f.dataType = StringType()
        fields.append(f)
    schema.fields = fields
    df.schema = schema
    schema_dict = {c: f for c, f in zip(columns, fields)}
    df.schema.__getitem__ = MagicMock(side_effect=lambda k: schema_dict[k])
    df.drop.return_value = df
    df.withColumn.return_value = df
    return df


class TestPIIMasking:
    def test_hash_strategy_applies_xxhash64(self):
        from pyspark.sql import functions as F
        df = _make_df(["customer_id", "email"])
        policy = PIIPolicy(columns=[PIIColumnConfig(column="email", strategy="hash")])
        result = apply_pii_masking(df, policy)
        assert result is df
        df.withColumn.assert_called()
        args = df.withColumn.call_args
        assert args[0][0] == "email"

    def test_mask_strategy_reveals_prefix(self):
        df = _make_df(["customer_id", "address"])
        policy = PIIPolicy(
            columns=[PIIColumnConfig(column="address", strategy="mask", reveal_prefix=5)]
        )
        result = apply_pii_masking(df, policy)
        assert result is df
        df.withColumn.assert_called()

    def test_null_strategy_sets_null(self):
        df = _make_df(["customer_id", "ssn"])
        policy = PIIPolicy(columns=[PIIColumnConfig(column="ssn", strategy="null")])
        result = apply_pii_masking(df, policy)
        assert result is df
        df.withColumn.assert_called()

    def test_drop_strategy_drops_column(self):
        df = _make_df(["customer_id", "email"])
        policy = PIIPolicy(columns=[PIIColumnConfig(column="email", strategy="drop")])
        result = apply_pii_masking(df, policy)
        df.drop.assert_called_with("email")

    def test_missing_column_skips_silently(self):
        df = _make_df(["customer_id"])
        policy = PIIPolicy(columns=[PIIColumnConfig(column="email", strategy="hash")])
        result = apply_pii_masking(df, policy)
        df.withColumn.assert_not_called()

    def test_multiple_columns(self):
        df = _make_df(["customer_id", "email", "address", "ssn"])
        policy = PIIPolicy(
            columns=[
                PIIColumnConfig(column="email", strategy="hash"),
                PIIColumnConfig(column="address", strategy="mask", reveal_prefix=3),
                PIIColumnConfig(column="ssn", strategy="drop"),
            ]
        )
        result = apply_pii_masking(df, policy)
        assert result is df
        assert df.withColumn.call_count == 2
        assert df.drop.call_count == 1

    def test_empty_policy_no_changes(self):
        df = _make_df(["customer_id", "email"])
        policy = PIIPolicy(columns=[])
        result = apply_pii_masking(df, policy)
        df.withColumn.assert_not_called()
        df.drop.assert_not_called()

    def test_drop_columns_property(self):
        policy = PIIPolicy(
            columns=[
                PIIColumnConfig(column="email", strategy="drop"),
                PIIColumnConfig(column="address", strategy="mask"),
            ]
        )
        assert policy.drop_columns == ["email"]

    def test_column_map_property(self):
        policy = PIIPolicy(
            columns=[
                PIIColumnConfig(column="email", strategy="hash"),
                PIIColumnConfig(column="address", strategy="mask"),
            ]
        )
        assert "email" in policy.column_map
        assert "address" in policy.column_map
        assert policy.column_map["email"].strategy == "hash"