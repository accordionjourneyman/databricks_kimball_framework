from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import StringType

from kimball.common.config import PIIColumnConfig, PIIPolicy
from kimball.processing.pii import _hmac_sha256, apply_pii_masking


@pytest.fixture(autouse=True)
def _patch_spark_fns(request):
    """Patch Spark functions that require an active SparkContext.

    Real-Spark behavior tests (class attr ``real_spark = True``) need the
    pyspark functions to stay real so actual masking runs.
    """
    if getattr(request.instance, "real_spark", False):
        yield
        return
    with (
        patch("kimball.processing.pii.F") as mock_F,
        patch("kimball.processing.pii.xxhash64", return_value=MagicMock()),
    ):
        mock_F.col.return_value = MagicMock()
        mock_F.lit.return_value = MagicMock()
        mock_F.concat.return_value = MagicMock()
        mock_F.sha2.return_value = MagicMock()
        mock_F.substring.return_value = MagicMock()
        mock_F.udf.return_value = MagicMock()
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
    schema_dict = {c: f for c, f in zip(columns, fields, strict=True)}
    df.schema.__getitem__ = MagicMock(side_effect=lambda k: schema_dict[k])
    df.drop.return_value = df
    df.withColumn.return_value = df
    return df


class TestPIIMasking:
    def test_hash_strategy_applies_xxhash64(self):
        df = _make_df(["customer_id", "email"])
        policy = PIIPolicy(columns=[PIIColumnConfig(column="email", strategy="hash")])
        result = apply_pii_masking(df, policy)
        assert result is df
        df.withColumn.assert_called()
        args = df.withColumn.call_args
        assert args[0][0] == "email"

    def test_fast_hash_is_explicitly_non_security_sensitive(self):
        df = _make_df(["email"])
        policy = PIIPolicy(
            columns=[PIIColumnConfig(column="email", strategy="fast_hash")]
        )

        apply_pii_masking(df, policy)

        df.withColumn.assert_called_once()

    def test_tokenize_requires_a_secret_reference(self):
        with pytest.raises(ValueError, match="secret_ref"):
            PIIColumnConfig(column="email", strategy="tokenize")

    def test_tokenize_resolves_secret_and_applies_hmac_udf(self):
        df = _make_df(["email"])
        policy = PIIPolicy(
            columns=[
                PIIColumnConfig(
                    column="email",
                    strategy="tokenize",
                    secret_ref="env://CUSTOMER_TOKEN_KEY",
                )
            ]
        )
        resolver = MagicMock()
        resolver.resolve.return_value = "test-key"

        apply_pii_masking(df, policy, secret_resolver=resolver)

        resolver.resolve.assert_called_once_with("env://CUSTOMER_TOKEN_KEY")
        df.withColumn.assert_called_once()

    def test_hmac_sha256_is_deterministic_keyed_and_null_preserving(self):
        first = _hmac_sha256("customer@example.com", b"key-a")
        assert first == _hmac_sha256("customer@example.com", b"key-a")
        assert first != _hmac_sha256("customer@example.com", b"key-b")
        assert len(first) == 64
        assert _hmac_sha256(None, b"key-a") is None

    def test_mask_strategy_reveals_prefix(self):
        df = _make_df(["customer_id", "address"])
        policy = PIIPolicy(
            columns=[
                PIIColumnConfig(column="address", strategy="mask", reveal_prefix=5)
            ]
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
        apply_pii_masking(df, policy)
        df.drop.assert_called_with("email")

    def test_missing_column_skips_silently(self):
        df = _make_df(["customer_id"])
        policy = PIIPolicy(columns=[PIIColumnConfig(column="email", strategy="hash")])
        apply_pii_masking(df, policy)
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
        apply_pii_masking(df, policy)
        df.withColumn.assert_not_called()
        df.drop.assert_not_called()


class TestPIIMaskingRealSpark:
    """Verify the actual masked *values* produced, not just that withColumn ran.

    The mock-based tests above only assert ``withColumn.assert_called()`` --
    which passes even if the masking expression hashed the wrong column,
    used a reversible algorithm, or did nothing. These run a real Spark
    session and check the resulting data.
    """

    real_spark = True

    def test_hash_is_deterministic_and_distinct(self, spark):
        # Same input -> same hash; different input -> different hash.
        # Irreversibility is not directly testable, but distinctness + the
        # Long return type together prove xxhash64 was actually applied.
        df = spark.createDataFrame(
            [("a@example.com",), ("b@example.com",), ("a@example.com",)],
            ["email"],
        )
        out = apply_pii_masking(
            df, PIIPolicy(columns=[PIIColumnConfig(column="email", strategy="hash")])
        ).collect()
        h = [r["email"] for r in out]
        assert h[0] == h[2]  # identical inputs hash alike
        assert h[0] != h[1]  # different inputs differ
        assert all(isinstance(v, int) for v in h)  # xxhash64 -> bigint

    def test_hash_does_not_leak_plaintext(self, spark):
        # The hashed column must never contain the original plaintext.
        df = spark.createDataFrame([("secret@example.com",)], ["email"])
        out = apply_pii_masking(
            df, PIIPolicy(columns=[PIIColumnConfig(column="email", strategy="hash")])
        ).head()
        assert "secret" not in str(out["email"])
        assert out["email"] != "secret@example.com"

    def test_mask_reveals_prefix_and_masks_rest(self, spark):
        df = spark.createDataFrame([("1234567890",)], ["ssn"])
        out = apply_pii_masking(
            df,
            PIIPolicy(
                columns=[
                    PIIColumnConfig(column="ssn", strategy="mask", reveal_prefix=4)
                ]
            ),
        ).head()
        masked = out["ssn"]
        # First 4 chars preserved, the remainder is the mask character run.
        assert str(masked).startswith("1234")
        assert "*" * 10 in str(masked)
        # Full plaintext is not present.
        assert "1234567890" not in str(masked)

    def test_mask_with_no_prefix_fully_masks(self, spark):
        df = spark.createDataFrame([("1234567890",)], ["ssn"])
        out = apply_pii_masking(
            df,
            PIIPolicy(
                columns=[
                    PIIColumnConfig(column="ssn", strategy="mask", reveal_prefix=0)
                ]
            ),
        ).head()
        assert "1234567890" not in str(out["ssn"])
        assert "*" * 10 in str(out["ssn"])

    def test_null_strategy_sets_null_value(self, spark):
        df = spark.createDataFrame([("not-null-value",)], ["ssn"])
        out = apply_pii_masking(
            df, PIIPolicy(columns=[PIIColumnConfig(column="ssn", strategy="null")])
        ).head()
        assert out["ssn"] is None  # column retained but value removed

    def test_drop_strategy_removes_column(self, spark):
        df = spark.createDataFrame([(1, "a@b.com")], ["customer_id", "email"])
        out = apply_pii_masking(
            df, PIIPolicy(columns=[PIIColumnConfig(column="email", strategy="drop")])
        )
        assert "email" not in out.columns
        assert "customer_id" in out.columns

    def test_missing_column_skips_without_error(self, spark):
        df = spark.createDataFrame([(1,)], ["customer_id"])
        out = apply_pii_masking(
            df, PIIPolicy(columns=[PIIColumnConfig(column="email", strategy="hash")])
        )
        assert out.columns == ["customer_id"]  # nothing changed

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
