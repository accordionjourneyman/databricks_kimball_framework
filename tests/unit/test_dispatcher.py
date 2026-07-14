"""Tests for the merge dispatcher retry logic."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.processing.dispatcher import merge, _is_concurrent_exception


@pytest.fixture(autouse=True)
def _patch_spark_fns():
    with patch("kimball.processing.dispatcher.current_timestamp", return_value=MagicMock()), \
         patch("kimball.processing.dispatcher.lit", return_value=MagicMock()):
        yield


class TestMergeRetry:
    def test_retries_on_concurrent_exception(self):
        from pyspark.errors.exceptions.base import PySparkException
        df = MagicMock()
        df.withColumn.return_value = df
        call_count = 0

        def flaky_merge(_df, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise PySparkException("ConcurrentAppendException: conflict")

        with patch("kimball.processing.dispatcher.merge_scd1", side_effect=flaky_merge), \
             patch("kimball.processing.dispatcher.time.sleep"):
            merge(df, target_table_name="t", join_keys=["id"], scd_type=1, max_retries=3)
        assert call_count == 3

    def test_does_not_retry_non_concurrent(self):
        from pyspark.errors.exceptions.base import PySparkException
        df = MagicMock()
        df.withColumn.return_value = df

        with patch("kimball.processing.dispatcher.merge_scd1",
                   side_effect=PySparkException("some other error")):
            with pytest.raises(PySparkException, match="some other error"):
                merge(df, target_table_name="t", join_keys=["id"], scd_type=1, max_retries=3)

    def test_exhausts_retries_and_raises(self):
        from pyspark.errors.exceptions.base import PySparkException
        df = MagicMock()
        df.withColumn.return_value = df

        with patch("kimball.processing.dispatcher.merge_scd1",
                   side_effect=PySparkException("ConcurrentAppendException: conflict")):
            with patch("kimball.processing.dispatcher.time.sleep"):
                with pytest.raises(PySparkException, match="ConcurrentAppendException"):
                    merge(df, target_table_name="t", join_keys=["id"], scd_type=1, max_retries=2)

    def test_adds_audit_columns(self):
        df = MagicMock()
        df.withColumn.return_value = df

        with patch("kimball.processing.dispatcher.merge_scd1") as mock_scd1:
            merge(df, target_table_name="t", join_keys=["id"], scd_type=1, batch_id="batch-123")
        assert df.withColumn.call_count == 2
        mock_scd1.assert_called_once()
        call_df = mock_scd1.call_args[0][0]
        assert call_df is df

    def test_unsupported_scd_type_raises(self):
        df = MagicMock()
        df.withColumn.return_value = df
        with pytest.raises(ValueError, match="Unsupported SCD type"):
            merge(df, target_table_name="t", join_keys=["id"], scd_type=3)

    def test_scd4_without_history_raises(self):
        df = MagicMock()
        df.withColumn.return_value = df
        with pytest.raises(ValueError, match="history_table"):
            merge(df, target_table_name="t", join_keys=["id"], scd_type=4)

    def test_scd6_without_current_values_raises(self):
        df = MagicMock()
        df.withColumn.return_value = df
        with pytest.raises(ValueError, match="current_value_columns"):
            merge(df, target_table_name="t", join_keys=["id"], scd_type=6)