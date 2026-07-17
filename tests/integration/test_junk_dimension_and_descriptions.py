"""Real Delta coverage for managed junk dimensions and YAML-owned comments."""

import pytest
from pyspark.sql import SparkSession

from kimball.common.config import JunkDimensionConfig
from kimball.orchestration.services.descriptions import DescriptionManager
from kimball.processing.junk_dimensions import materialize_junk_dimensions

pytestmark = pytest.mark.usefixtures("spark")


def test_junk_dimension_is_idempotent_and_adds_fact_fk(
    spark: SparkSession, test_db: str
) -> None:
    table = f"{test_db}.dim_order_flags"
    definition = JunkDimensionConfig(
        dimension_table=table,
        surrogate_key="order_flags_sk",
        source_columns=["is_gift", "is_priority"],
    )
    first = spark.createDataFrame(
        [(1, True, False), (2, False, False), (3, True, False)],
        "order_id long, is_gift boolean, is_priority boolean",
    )

    enriched = materialize_junk_dimensions(spark, first, [definition])
    assert "order_flags_sk" in enriched.columns
    assert spark.table(table).count() == 2

    second = spark.createDataFrame(
        [(4, True, False), (5, False, True)],
        "order_id long, is_gift boolean, is_priority boolean",
    )
    materialize_junk_dimensions(spark, second, [definition])
    assert spark.table(table).count() == 3


def test_description_manifest_avoids_reissuing_unchanged_ddl(
    spark: SparkSession, test_db: str
) -> None:
    table = f"{test_db}.described_table"
    spark.sql(f"CREATE TABLE {table} (id BIGINT, name STRING) USING DELTA")
    manager = DescriptionManager()

    assert manager.sync(spark, table, "People", {"name": "Display name"}) is True
    assert manager.sync(spark, table, "People", {"name": "Display name"}) is False
    fields = {field.name: field for field in spark.table(table).schema.fields}
    assert fields["name"].metadata.get("comment") == "Display name"
