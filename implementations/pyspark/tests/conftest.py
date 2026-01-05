import sys
from unittest.mock import MagicMock

# Mock databricks.sdk.runtime to allow local testing without credentials
# This must be done before any imports that trigger the auth check
if "databricks.sdk.runtime" not in sys.modules:
    mock_db_sdk = MagicMock()
    # Mock spark session
    mock_db_sdk.spark = MagicMock()
    # Mock dbutils which is often accessed via get_dbutils or similar
    mock_db_sdk.dbutils = MagicMock()

    sys.modules["databricks.sdk.runtime"] = mock_db_sdk
