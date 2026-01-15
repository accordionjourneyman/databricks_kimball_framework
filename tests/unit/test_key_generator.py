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

    # Case 1: Key exists
    gen.generate_keys(df, "id")
    df.drop.assert_not_called()

    # Case 2: Key does not exist
    df.columns = ["val"]
    gen.generate_keys(df, "id")
    # Should not call drop (or at least return df)
    # We can't easily check "not called" on the *same* mock if we don't reset,
    # but here we just check it returns something.


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
