"""Real-Spark verification of keyed PII tokenization."""

import pytest
from pyspark.sql import SparkSession

from kimball.common.config import PIIColumnConfig, PIIPolicy
from kimball.common.secrets import SecretResolver
from kimball.processing.pii import apply_pii_masking

pytestmark = pytest.mark.usefixtures("spark")


def test_hmac_tokenization_is_deterministic_keyed_and_null_preserving(
    spark: SparkSession,
) -> None:
    values = [("a@example.com",), ("a@example.com",), (None,)]
    source = spark.createDataFrame(values, "email string")
    policy = PIIPolicy(
        columns=[
            PIIColumnConfig(
                column="email",
                strategy="tokenize",
                secret_ref="env://PII_KEY",
            )
        ]
    )

    first = apply_pii_masking(
        source,
        policy,
        secret_resolver=SecretResolver(environ={"PII_KEY": "key-a"}),
    ).collect()
    second = apply_pii_masking(
        source,
        policy,
        secret_resolver=SecretResolver(environ={"PII_KEY": "key-b"}),
    ).collect()

    assert first[0]["email"] == first[1]["email"]
    assert len(first[0]["email"]) == 64
    assert first[0]["email"] != second[0]["email"]
    assert first[2]["email"] is None
