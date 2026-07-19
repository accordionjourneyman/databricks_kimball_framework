"""Managed junk-dimension materialisation for fact pipelines."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from kimball.common.config import JunkDimensionConfig


def materialize_junk_dimensions(
    spark: SparkSession, fact_df: DataFrame, definitions: list[JunkDimensionConfig]
) -> DataFrame:
    """Upsert distinct low-cardinality combinations and add their keys to a fact."""
    result = fact_df
    for definition in definitions:
        missing = set(definition.source_columns).difference(result.columns)
        if missing:
            raise ValueError(
                f"Junk dimension '{definition.dimension_table}' is missing source columns: {sorted(missing)}"
            )
        key_expr = F.xxhash64(
            *[
                F.coalesce(F.col(c).cast("string"), F.lit("__NULL__"))
                for c in definition.source_columns
            ]
        )
        result = result.withColumn(definition.surrogate_key, key_expr)
        combinations = result.select(
            definition.surrogate_key, *definition.source_columns
        ).dropDuplicates()
        if spark.catalog.tableExists(definition.dimension_table):
            existing_table = spark.table(definition.dimension_table)
            required = {definition.surrogate_key, *definition.source_columns}
            missing_target = required.difference(existing_table.columns)
            if missing_target:
                raise ValueError(
                    f"Junk dimension '{definition.dimension_table}' has an incompatible "
                    f"schema; missing columns: {sorted(missing_target)}"
                )
            existing = existing_table.select(
                definition.surrogate_key, *definition.source_columns
            )
            joined = combinations.alias("incoming").join(
                existing.alias("existing"), definition.surrogate_key, "inner"
            )
            collision = None
            for column in definition.source_columns:
                differs = ~F.col(f"incoming.{column}").eqNullSafe(
                    F.col(f"existing.{column}")
                )
                collision = differs if collision is None else collision | differs
            if collision is not None and not joined.filter(collision).isEmpty():
                raise ValueError(
                    f"Hash collision detected in junk dimension "
                    f"'{definition.dimension_table}'"
                )
            target = DeltaTable.forName(spark, definition.dimension_table)
            target.alias("target").merge(
                combinations.alias("source"),
                f"target.`{definition.surrogate_key}` = "
                f"source.`{definition.surrogate_key}`",
            ).whenNotMatchedInsertAll().execute()
        else:
            combinations.write.format("delta").mode("error").saveAsTable(
                definition.dimension_table
            )
    return result
