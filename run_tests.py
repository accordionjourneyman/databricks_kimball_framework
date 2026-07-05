"""
Runner script for Databricks integration tests.

This script is uploaded to the workspace by run_databricks_tests.py
and executed on the cluster. It accepts:
  sys.argv[1] = test path (default: tests/integration)
  sys.argv[2] = catalog name (default: spark_catalog)
  sys.argv[3:] = extra pytest arguments (e.g. --scale small)
"""

import os
import sys

test_path = sys.argv[1] if len(sys.argv) > 1 else "tests/integration"
catalog = sys.argv[2] if len(sys.argv) > 2 else "spark_catalog"
extra_args = sys.argv[3:] if len(sys.argv) > 3 else []
os.environ.setdefault("KIMBALL_TEST_CATALOG", catalog)

os.environ.setdefault("DATABRICKS_HOST", "")
os.environ.setdefault("DATABRICKS_TOKEN", "")

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
sys.dont_write_bytecode = True

import pytest
exit_code = pytest.main([test_path, "-v", "-p", "no:cacheprovider"] + extra_args)
if exit_code != 0:
    sys.exit(exit_code)
