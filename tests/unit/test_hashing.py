from unittest.mock import MagicMock, patch

from kimball.hashing import compute_hashdiff


@patch("kimball.hashing.xxhash64")
@patch("kimball.hashing.col")
@patch("kimball.hashing.coalesce")
@patch("kimball.hashing.lit")
def test_compute_hashdiff_logic(mock_lit, mock_coalesce, mock_col, mock_xxhash64):
    # Setup mocks to return distinguishable objects
    mock_col.side_effect = lambda x: MagicMock(name=f"col({x})")
    mock_lit.side_effect = lambda x: MagicMock(name=f"lit({x})")

    # Call function
    cols = ["name", "city"]
    result = compute_hashdiff(cols)

    # Verify calls
    # We expect col(c).cast("string") -> coalesce(..., lit("")) -> xxhash64(...)

    # Check col calls
    assert mock_col.call_count == 2
    mock_col.assert_any_call("name")
    mock_col.assert_any_call("city")

    # Check coalesce calls
    assert mock_coalesce.call_count == 2

    # Check xxhash64 call
    assert mock_xxhash64.call_count == 1
