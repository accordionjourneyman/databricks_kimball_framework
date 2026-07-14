"""
Unit tests for hashing utilities.

Tests hashdiff computation for SCD2 change detection.
"""

from unittest.mock import MagicMock, patch


@patch("kimball.processing.hashing.xxhash64")
@patch("kimball.processing.hashing.concat_ws")
@patch("kimball.processing.hashing.when")
@patch("kimball.processing.hashing.col")
@patch("kimball.processing.hashing.lit")
def test_compute_hashdiff_logic(
    mock_lit, mock_col, mock_when, mock_concat_ws, mock_xxhash64
):
    """Test hashdiff computation with mocked Spark functions."""
    from kimball.processing.hashing import compute_hashdiff

    mock_col.side_effect = lambda x: MagicMock(name=f"col({x})")
    mock_lit.side_effect = lambda x: MagicMock(name=f"lit({x})")
    mock_when.side_effect = lambda *args: MagicMock(name="when")
    mock_concat_ws.side_effect = lambda *args: MagicMock(name="concat_ws")
    mock_xxhash64.side_effect = lambda *args: MagicMock(name="xxhash64")

    cols = ["name", "city"]
    _ = compute_hashdiff(cols)

    assert mock_col.call_count == 4
    mock_col.assert_any_call("name")
    mock_col.assert_any_call("city")
    assert mock_when.call_count == 2
    assert mock_concat_ws.call_count == 1
    assert mock_concat_ws.call_args[0][0] == "\x00"
    assert mock_xxhash64.call_count == 1