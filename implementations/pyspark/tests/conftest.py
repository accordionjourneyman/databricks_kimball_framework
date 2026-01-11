"""Pytest configuration for Kimball Framework unit tests.

Mocks Databricks SDK components to allow testing outside of Databricks environment.
"""

import sys
from unittest.mock import MagicMock

# Mock the databricks.sdk.runtime module BEFORE any kimball imports
# This prevents the import error when running tests locally
mock_runtime = MagicMock()
mock_runtime.spark = MagicMock()
sys.modules["databricks"] = MagicMock()
sys.modules["databricks.sdk"] = MagicMock()
sys.modules["databricks.sdk.runtime"] = mock_runtime
