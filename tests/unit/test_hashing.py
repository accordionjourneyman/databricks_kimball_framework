"""
Unit tests for hashing utilities.

Tests hashdiff computation for SCD2 change detection.
"""

from unittest.mock import MagicMock, patch


@patch("kimball.processing.hashing.sha2")
@patch("kimball.processing.hashing.concat_ws")
@patch("kimball.processing.hashing.when")
@patch("kimball.processing.hashing.col")
@patch("kimball.processing.hashing.lit")
def test_compute_hashdiff_logic(
    mock_lit, mock_col, mock_when, mock_concat_ws, mock_sha2
):
    """Test hashdiff computation with mocked Spark functions."""
    from kimball.processing.hashing import compute_hashdiff

    # Setup mocks to return distinguishable objects
    mock_col.side_effect = lambda x: MagicMock(name=f"col({x})")
    mock_lit.side_effect = lambda x: MagicMock(name=f"lit({x})")
    mock_when.side_effect = lambda *args: MagicMock(name="when")
    mock_concat_ws.side_effect = lambda *args: MagicMock(name="concat_ws")
    mock_sha2.side_effect = lambda *args: MagicMock(name="sha2")

    # Call function
    cols = ["name", "city"]
    _ = compute_hashdiff(cols)

    # Verify calls
    # We expect col(c).isNull() AND col(c).cast("string") per column (2 calls each)

    # Check col calls (two per column: isNull check + cast fallback)
    assert mock_col.call_count == 4  # 2 columns * 2 calls each
    mock_col.assert_any_call("name")
    mock_col.assert_any_call("city")

    # Check when calls (one per column for NULL handling)
    assert mock_when.call_count == 2

    # Check concat_ws call (one call with separator and all normalized columns)
    assert mock_concat_ws.call_count == 1
    assert mock_concat_ws.call_args[0][0] == "\x00"  # Check null byte separator

    # Check sha2 call (wraps the concat_ws result)
    assert mock_sha2.call_count == 1
    assert mock_sha2.call_args[0][1] == 256  # Check bit length
