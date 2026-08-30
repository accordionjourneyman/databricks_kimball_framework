#!/usr/bin/env python3
"""Show the fundamental gap between SDP and the framework's processing model."""

import subprocess
import sys

subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q", "pyspark[pipelines]==4.2.0"],
    check=False,
)


print("SDP decorators require functions that RETURN DataFrames")
print("Functions are evaluated during pipeline PLANNING, not just execution")
print()
print("Forbidden inside SDP functions:")
print("  - save(), saveAsTable(), toTable()")
print("  - collect(), count(), pivot()")
print("  - DeltaTable.forName(...).merge(...).execute()")
print()
print("merge_scd2() calls DeltaTable.forName().merge().execute()")
print("  -> 63 DeltaTable/merge/execute calls in scd2.py")
print("  -> CANNOT run inside @dp.table / @dp.materialized_view")
print()
print("create_auto_cdc_flow works because it's a DECLARATIVE flow")
print("  - SDP handles the write internally, not in user code")
print("  - Only SCD1 is supported in open-source")
print("  - SCD2 requires Databricks Lakeflow")
print()
print("Conclusion: SDP can't replace the framework's merge logic")
print("The framework processes data imperatively (MERGE INTO)")
print("SDP processes data declaratively (CREATE OR REFRESH)")
print("Different paradigms for different problems")
