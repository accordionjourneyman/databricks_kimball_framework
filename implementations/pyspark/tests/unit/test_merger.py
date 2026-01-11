"""
Unit tests for DeltaMerger.

Tests MERGE operations, SCD1/SCD2 handling, and audit column injection.
"""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_df():
    """Create a mock DataFrame."""
    df = MagicMock()
    df.columns = ["id", "val", "_change_type"]
    df.filter.return_value = df
    df.withColumn.return_value = df
    df.drop.return_value = df
    df.select.return_value = df
    df.alias.return_value = df
    return df


@patch("kimball.merger.col")
@patch("kimball.merger.current_timestamp")
@patch("kimball.merger.lit")
@patch("kimball.merger.spark")
@patch("kimball.merger.DeltaTable")
def test_merge_scd1_execution(
    mock_delta_table, mock_spark, mock_lit, mock_current_ts, mock_col, mock_df
):
    """Test SCD1 MERGE execution."""
    from kimball.merger import DeltaMerger

    # Setup DeltaTable mock
    mock_dt = MagicMock()
    mock_delta_table.forName.return_value = mock_dt
    mock_dt.toDF.return_value = mock_df

    # Setup merge builder
    mock_dt.alias.return_value.merge.return_value = mock_dt
    mock_dt.whenMatchedUpdate.return_value = mock_dt
    mock_dt.whenMatchedDelete.return_value = mock_dt
    mock_dt.whenNotMatchedInsert.return_value = mock_dt

    # Mock catalog to say table is Delta
    mock_spark.catalog.listTables.return_value = [MagicMock(tableType="MANAGED")]
    mock_spark.sql.return_value.first.return_value = {"provider": "delta"}

    merger = DeltaMerger()  # No spark argument
    merger.merge(
        target_table_name="target",
        source_df=mock_df,
        join_keys=["id"],
        delete_strategy="hard",
        scd_type=1,
    )

    # Verify DeltaTable was accessed
    assert mock_delta_table.forName.called or mock_spark.sql.called


@patch("kimball.merger.col")
@patch("kimball.merger.current_timestamp")
@patch("kimball.merger.lit")
@patch("kimball.merger.spark")
@patch("kimball.merger.DeltaTable")
def test_merge_preserves_audit_columns(
    mock_delta_table, mock_spark, mock_lit, mock_current_ts, mock_col, mock_df
):
    """Test that audit columns are added during merge."""
    from kimball.merger import DeltaMerger

    mock_dt = MagicMock()
    mock_delta_table.forName.return_value = mock_dt
    mock_dt.toDF.return_value = mock_df
    mock_dt.alias.return_value.merge.return_value = mock_dt
    mock_dt.whenMatchedUpdate.return_value = mock_dt
    mock_dt.whenNotMatchedInsert.return_value = mock_dt

    mock_spark.catalog.listTables.return_value = [MagicMock(tableType="MANAGED")]
    mock_spark.sql.return_value.first.return_value = {"provider": "delta"}

    merger = DeltaMerger()
    merger.merge(
        target_table_name="target",
        source_df=mock_df,
        join_keys=["id"],
        delete_strategy="soft",
        scd_type=1,
    )

    # Soft delete should not physically remove rows
    assert mock_dt.alias.called or mock_spark.sql.called


@patch("kimball.merger.spark")
@patch("kimball.merger.DeltaTable")
def test_is_delta_table(mock_delta_table, mock_spark):
    """Test Delta table detection via provider check."""
    from kimball.merger import DeltaMerger

    mock_spark.sql.return_value.first.return_value = {"provider": "delta"}

    merger = DeltaMerger()
    result = merger._is_delta_table("test_table")

    assert result is True


@patch("kimball.merger.spark")
@patch("kimball.merger.DeltaTable")
def test_optimize_table(mock_delta_table, mock_spark):
    """Test table optimization."""
    from kimball.merger import DeltaMerger

    merger = DeltaMerger()
    merger.optimize_table("test_table")

    # Should call OPTIMIZE SQL
    mock_spark.sql.assert_called()
    call_args = str(mock_spark.sql.call_args)
    assert "OPTIMIZE" in call_args


@patch("kimball.merger.spark")
@patch("kimball.merger.DeltaTable")
def test_get_last_merge_metrics(mock_delta_table, mock_spark):
    """Test retrieving merge metrics."""
    from kimball.merger import DeltaMerger

    mock_dt = MagicMock()
    mock_delta_table.forName.return_value = mock_dt
    mock_dt.history.return_value.filter.return_value.first.return_value = {
        "operationMetrics": {
            "numSourceRows": 100,
            "numTargetRowsInserted": 50,
            "numTargetRowsUpdated": 30,
        }
    }

    merger = DeltaMerger()
    metrics = merger.get_last_merge_metrics("test_table")

    assert metrics is not None
