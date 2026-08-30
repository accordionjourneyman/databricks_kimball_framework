from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestGetSpark:
    def test_returns_databricks_spark_when_available(self):
        mock_spark = MagicMock()
        with patch.dict(
            "sys.modules", {"databricks.sdk.runtime": MagicMock(spark=mock_spark)}
        ):
            from kimball.common import spark_session
            from kimball.common.spark_session import get_spark

            spark_session._active_spark = None
            result = get_spark()
            assert result is mock_spark
