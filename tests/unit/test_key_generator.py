from unittest.mock import MagicMock, patch

from kimball.processing.key_generator import HashKeyGenerator


def test_hash_key_generator_mock():
    with (
        patch("kimball.processing.key_generator.xxhash64") as mock_hash,
        patch("kimball.processing.key_generator.col"),
    ):
        gen = HashKeyGenerator(["val"])
        df = MagicMock()

        gen.generate_keys(df, "sk")

        df.withColumn.assert_called()
        args = df.withColumn.call_args
        assert args[0][0] == "sk"
        mock_hash.assert_called()
