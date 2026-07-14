"""Run golden tests without Maven resolution (JARs already in pyspark/jars).

The Dockerfile pre-downloads delta JARs into pyspark/jars, so we don't need
configure_spark_with_delta_pip to download them again via Maven.
"""
import os
os.environ["SPARK_TESTING"] = "1"

import delta
delta.configure_spark_with_delta_pip = lambda builder, **kw: builder

import pytest
raise SystemExit(pytest.main([
    "tests/golden/test_golden.py", "-v", "--tb=long", "-x",
]))
