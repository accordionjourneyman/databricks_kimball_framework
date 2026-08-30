"""Warmup script for serverless job compute session.

Runs a trivial Spark query to trigger cold-start initialization
so subsequent tasks in the same job reuse the warm session.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.sql("SELECT 1 AS ready")
print(f"Warmup complete: {df.collect()[0]['ready']}")
