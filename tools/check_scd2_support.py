#!/usr/bin/env python3
"""Check SCD Type 2 support in open-source Spark SDP."""

# ruff: noqa: E402  # installs the probe dependency before importing it
import subprocess
import sys

subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q", "pyspark[pipelines]==4.2.0"],
    check=False,
)

import pyspark

print("PySpark:", pyspark.__version__)
import inspect

from pyspark.pipelines.api import create_auto_cdc_flow

sig = inspect.signature(create_auto_cdc_flow)
param = sig.parameters["stored_as_scd_type"]
print("stored_as_scd_type annotation:", param.annotation)
print("stored_as_scd_type default:", param.default)

source = inspect.getsource(create_auto_cdc_flow)
for i, line in enumerate(source.split("\n")):
    s = line.strip()
    if any(kw in s.lower() for kw in ["scd", "stored_as", "literal", "raise"]):
        print(f"  L{i}: {s}")
