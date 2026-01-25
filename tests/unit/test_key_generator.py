from unittest.mock import MagicMock, patch

from kimball.processing.key_generator import (
    HashKeyGenerator,
    IdentityKeyGenerator,
    SequenceKeyGenerator,
)


def test_identity_key_generator_mock():
    gen = IdentityKeyGenerator()
    df = MagicMock()
    df.columns = ["id", "val"]

    # Case 1: Key exists - DataFrame should be returned unchanged
    # This allows explicit values for default rows (-1, -2, -3)
    result = gen.generate_keys(df, "id")
    assert result is df, "IdentityKeyGenerator should return DataFrame unchanged when key exists"

    # Case 2: Key does not exist - DataFrame should still be returned unchanged
    # Delta Lake will auto-generate the identity column on INSERT
    df.columns = ["val"]
    result = gen.generate_keys(df, "id")
    assert result is df, "IdentityKeyGenerator should return DataFrame unchanged when key is missing"


def test_hash_key_generator_mock():
    with (
        patch("kimball.processing.key_generator.xxhash64") as mock_hash,
        patch("kimball.processing.key_generator.col"),
    ):
        gen = HashKeyGenerator(["val"])
        df = MagicMock()

        gen.generate_keys(df, "sk")

        # Should call withColumn with "sk" and result of xxhash64
        df.withColumn.assert_called()
        args = df.withColumn.call_args
        assert args[0][0] == "sk"

        # Should call xxhash64
        mock_hash.assert_called()


def test_sequence_key_generator_mock():
    """SequenceKeyGenerator is blocked by default - verify it raises."""
    import pytest

    gen = SequenceKeyGenerator()
    df = MagicMock()

    with pytest.raises(RuntimeError, match="BLOCKED"):
        gen.generate_keys(df, "sk", existing_max_key=10)
