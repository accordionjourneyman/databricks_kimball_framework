"""Boot a minimal Spark+Delta session once at image build time.

The only purpose is Ivy resolution: `configure_spark_with_delta_pip`
downloads the Delta runtime JARs into ~/.ivy2 on the first session boot.
Baking them into the image removes ~35s of network resolution from every
cold container start. Run as the image user (same HOME the test session
uses) with SPARK_HOME set.

See Dockerfile; measured numbers in docs/adr/ADR-004.
"""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def main() -> None:
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("ivy-prebake")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    builder = configure_spark_with_delta_pip(builder)
    spark = builder.getOrCreate()
    spark.sql("SELECT 1").collect()
    spark.stop()
    print("ivy cache pre-baked")


if __name__ == "__main__":
    main()
