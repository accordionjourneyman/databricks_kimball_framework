from unittest.mock import MagicMock, patch

from kimball.processing.key_generator import HashKeyGenerator, type7_key_columns


def test_hash_key_generator_mock():
    with (
        patch("kimball.processing.key_generator.xxhash64") as mock_hash,
        patch("kimball.processing.key_generator.col"),
        patch("kimball.processing.key_generator.lit"),
        patch("kimball.processing.key_generator.coalesce"),
        patch("kimball.processing.key_generator.concat_ws"),
    ):
        gen = HashKeyGenerator(["val"])
        df = MagicMock()

        gen.generate_keys(df, "sk")

        df.withColumn.assert_called()
        args = df.withColumn.call_args
        assert args[0][0] == "sk"
        mock_hash.assert_called()


def test_type7_key_columns_use_durable_fingerprint_and_effective_time():
    with (
        patch("kimball.processing.key_generator.xxhash64") as mock_hash,
        patch("kimball.processing.key_generator.sha2") as mock_sha,
        patch("kimball.processing.key_generator.concat_ws"),
        patch("kimball.processing.key_generator.coalesce"),
        patch("kimball.processing.key_generator.lit"),
        patch("kimball.processing.key_generator.col"),
    ):
        mock_sha.return_value = MagicMock()
        columns = type7_key_columns(["source_system", "customer_id"], "updated_at")

    assert set(columns) == {
        "durable_key",
        "row_key",
        "durable_fingerprint",
        "row_fingerprint",
    }
    assert mock_hash.call_count == 2
