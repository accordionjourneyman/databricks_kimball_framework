from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.orchestration.transaction import TransactionManager


@pytest.fixture
def spark_mock():
    return MagicMock()


@pytest.fixture
def manager(spark_mock):
    return TransactionManager(spark_session=spark_mock)


class TestGetTableVersion:
    def test_returns_version_from_history(self, manager, spark_mock):
        delta_table = MagicMock()
        delta_table.history.return_value.collect.return_value = [{"version": 5}]
        with patch("kimball.orchestration.transaction.DeltaTable.forName", return_value=delta_table):
            result = manager._get_table_version("test_table")
        assert result == 5

    def test_returns_negative_one_when_no_history(self, manager, spark_mock):
        delta_table = MagicMock()
        delta_table.history.return_value.collect.return_value = []
        with patch("kimball.orchestration.transaction.DeltaTable.forName", return_value=delta_table):
            result = manager._get_table_version("test_table")
        assert result == -1

    def test_returns_negative_one_on_analysis_exception(self, manager, spark_mock):
        from pyspark.errors import AnalysisException
        with patch("kimball.orchestration.transaction.DeltaTable.forName", side_effect=AnalysisException("not found")):
            result = manager._get_table_version("test_table")
        assert result == -1


class TestRollback:
    def test_rollback_executes_restore(self, manager, spark_mock):
        manager._rollback("test_table", 3)
        spark_mock.sql.assert_called_once_with(
            "RESTORE TABLE `test_table` TO VERSION AS OF 3"
        )

    def test_rollback_raises_on_failure(self, manager, spark_mock):
        from pyspark.errors import PySparkException
        spark_mock.sql.side_effect = PySparkException("restore failed")
        with pytest.raises(PySparkException, match="restore failed"):
            manager._rollback("test_table", 3)


class TestRecoverZombies:
    def test_skips_when_table_not_exists(self, manager, spark_mock):
        spark_mock.catalog.tableExists.return_value = False
        result = manager.recover_zombies("test_table", "batch-1")
        assert result is False

    def test_returns_false_when_no_zombie_commits(self, manager, spark_mock):
        spark_mock.catalog.tableExists.return_value = True
        delta_table = MagicMock()
        delta_table.history.return_value.collect.return_value = [
            {"version": 1, "userMetadata": "other_batch"}
        ]
        with patch("kimball.orchestration.transaction.DeltaTable.forName", return_value=delta_table):
            result = manager.recover_zombies("test_table", "batch-1")
        assert result is False

    def test_rolls_back_zombie_commits(self, manager, spark_mock):
        spark_mock.catalog.tableExists.return_value = True
        delta_table = MagicMock()
        delta_table.history.return_value.collect.return_value = [
            {"version": 3, "userMetadata": "batch-1"},
            {"version": 2, "userMetadata": "other"},
        ]
        with patch("kimball.orchestration.transaction.DeltaTable.forName", return_value=delta_table):
            # Do NOT mock _rollback -- let the real implementation issue the
            # RESTORE statement so we verify the actual SQL (version 2 = the
            # version before the first zombie commit at version 3), not just
            # that _rollback was called with the right version.
            result = manager.recover_zombies("test_table", "batch-1")
        assert result is True
        spark_mock.sql.assert_called_once_with(
            "RESTORE TABLE `test_table` TO VERSION AS OF 2"
        )

    def test_handles_restore_below_zero(self, manager, spark_mock):
        spark_mock.catalog.tableExists.return_value = True
        delta_table = MagicMock()
        delta_table.history.return_value.collect.return_value = [
            {"version": 0, "userMetadata": "batch-1"}
        ]
        with patch("kimball.orchestration.transaction.DeltaTable.forName", return_value=delta_table):
            result = manager.recover_zombies("test_table", "batch-1")
        assert result is False

    def test_handles_general_exception(self, manager, spark_mock):
        from pyspark.errors import PySparkException
        spark_mock.catalog.tableExists.side_effect = PySparkException("boom")
        result = manager.recover_zombies("test_table", "batch-1")
        assert result is False


class TestTableTransaction:
    def test_successful_transaction(self, manager, spark_mock):
        manager._get_table_version = MagicMock(return_value=5)
        with manager.table_transaction("test_table", "batch-1"):
            pass
        spark_mock.conf.set.assert_called_once()
        spark_mock.conf.unset.assert_called_once()

    def test_rollback_on_exception_version_0(self, manager, spark_mock):
        manager._get_table_version = MagicMock(side_effect=[0, 2])
        manager._rollback = MagicMock()
        with pytest.raises(ValueError, match="test error"):
            with manager.table_transaction("test_table", "batch-1"):
                raise ValueError("test error")
        manager._rollback.assert_called_once_with("test_table", 0)

    def test_rollback_on_exception_positive_version(self, manager, spark_mock):
        manager._get_table_version = MagicMock(side_effect=[5, 7])
        manager._rollback = MagicMock()
        with pytest.raises(ValueError, match="test error"):
            with manager.table_transaction("test_table", "batch-1"):
                raise ValueError("test error")
        manager._rollback.assert_called_once_with("test_table", 5)

    def test_rollback_on_exception_negative_start(self, manager, spark_mock):
        manager._get_table_version = MagicMock(side_effect=[-1, 2])
        manager._rollback = MagicMock()
        with pytest.raises(ValueError, match="test error"):
            with manager.table_transaction("test_table", "batch-1"):
                raise ValueError("test error")
        manager._rollback.assert_not_called()

    def test_clears_metadata_on_success(self, manager, spark_mock):
        manager._get_table_version = MagicMock(return_value=5)
        with manager.table_transaction("test_table", "batch-1"):
            pass
        spark_mock.conf.unset.assert_called_once_with(
            "spark.databricks.delta.commitInfo.userMetadata"
        )

    def test_handles_conf_set_failure(self, manager, spark_mock):
        from pyspark.errors import PySparkException
        spark_mock.conf.set.side_effect = PySparkException("cannot set")
        manager._get_table_version = MagicMock(return_value=5)
        with manager.table_transaction("test_table", "batch-1"):
            pass
        assert True
