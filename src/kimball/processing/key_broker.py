from __future__ import annotations

import logging
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)

from kimball.common.config import ForeignKeyConfig, NullPolicyConfig
from kimball.common.constants import DEFAULT_VALID_FROM, DEFAULT_VALID_TO
from kimball.common.errors import DataQualityError
from kimball.observability.unresolved_keys import UnresolvedKeyRegistry
from kimball.processing.identity_map import load_validated_identity_map
from kimball.processing.key_generator import HashKeyGenerator

logger = logging.getLogger(__name__)


def _any_null(columns: list[str]) -> Column:
    return reduce(
        lambda left, right: left | right, (F.col(c).isNull() for c in columns)
    )


def _placeholder(field, *, status: str = "NOT_YET_AVAILABLE") -> Column:
    data_type = field.dataType
    if isinstance(data_type, StringType):
        value = "Not Yet Available"
    elif isinstance(data_type, (IntegerType, LongType, ShortType)):
        value = -3
    elif isinstance(data_type, DecimalType):
        value = -3
    elif isinstance(data_type, (DoubleType, FloatType)):
        value = -3.0
    elif isinstance(data_type, BooleanType):
        value = False
    elif isinstance(data_type, TimestampType):
        value = DEFAULT_VALID_FROM
    elif isinstance(data_type, DateType):
        value = DEFAULT_VALID_FROM.date()
    else:
        raise ValueError(
            f"Skeleton requires an explicit substitute for {field.name} ({data_type})"
        )
    return F.lit(value).cast(data_type)


class KeyBroker:
    """Resolve fact dimension keys before the fact table is mutated.

    Entity keys are generated only while materializing dimension members. Facts
    consume those keys through a set-based lookup and use reserved members for
    explicit exceptional states.
    """

    def __init__(
        self,
        spark: SparkSession,
        unresolved_registry: UnresolvedKeyRegistry | None = None,
    ):
        self.spark = spark
        self.unresolved_registry = unresolved_registry
        self._identity_maps: dict[str, DataFrame] = {}

    def resolve_fact_keys(
        self,
        fact_df: DataFrame,
        foreign_keys: list[ForeignKeyConfig],
        *,
        batch_id: str,
        null_policy: NullPolicyConfig,
        fact_table: str = "",
        fact_grain: list[str] | None = None,
        source_version: int = -1,
    ) -> DataFrame:
        resolved = fact_df
        for index, fk in enumerate(foreign_keys):
            if fk.lookup is None or fk.references is None:
                continue
            required_fact_columns = set(fk.lookup.source_columns)
            if fk.lookup.event_time:
                required_fact_columns.add(fk.lookup.event_time)
            missing_fact_columns = sorted(required_fact_columns - set(resolved.columns))
            if missing_fact_columns:
                raise DataQualityError(
                    f"Brokered relationship {fk.column} is missing fact columns: "
                    + ", ".join(missing_fact_columns)
                )
            resolved, original_identity_columns = self._apply_identity_map(
                resolved, fk, index
            )
            if fk.lookup.early_arriving == "skeleton":
                self._ensure_skeletons(resolved, fk, batch_id, null_policy)
            resolved = self._resolve_relationship(
                resolved,
                fk,
                index,
                original_identity_columns=original_identity_columns,
                fact_table=fact_table,
                fact_grain=fact_grain or [],
                batch_id=batch_id,
                source_version=source_version,
            )
        return resolved

    def _apply_identity_map(
        self,
        fact_df: DataFrame,
        fk: ForeignKeyConfig,
        index: int,
    ) -> tuple[DataFrame, list[str]]:
        lookup = fk.lookup
        assert lookup is not None
        original_columns: list[str] = []
        if not lookup.identity_map:
            return fact_df, original_columns
        source_column = lookup.source_columns[0]
        original_column = f"__identity_{index}_original"
        original_columns.append(original_column)
        event_time = lookup.event_time
        assert event_time is not None
        if lookup.identity_map not in self._identity_maps:
            self._identity_maps[lookup.identity_map] = load_validated_identity_map(
                self.spark, lookup.identity_map
            )
        mapping = self._identity_maps[lookup.identity_map].select(
            F.col("source_identity").alias(f"__identity_{index}_source"),
            F.col("canonical_identity").alias(f"__identity_{index}_canonical"),
            F.col("valid_from").alias(f"__identity_{index}_from"),
            F.col("valid_to").alias(f"__identity_{index}_to"),
        )
        with_original = fact_df.withColumn(original_column, F.col(source_column))
        condition = (
            (with_original[source_column] == mapping[f"__identity_{index}_source"])
            & (with_original[event_time] >= mapping[f"__identity_{index}_from"])
            & (with_original[event_time] < mapping[f"__identity_{index}_to"])
        )
        joined = with_original.join(mapping, condition, "left")
        joined = joined.withColumn(
            source_column,
            F.coalesce(
                F.col(f"__identity_{index}_canonical").cast(
                    fact_df.schema[source_column].dataType
                ),
                F.col(source_column),
            ),
        )
        return (
            joined.drop(
                f"__identity_{index}_source",
                f"__identity_{index}_canonical",
                f"__identity_{index}_from",
                f"__identity_{index}_to",
            ),
            original_columns,
        )

    def _ensure_skeletons(
        self,
        fact_df: DataFrame,
        fk: ForeignKeyConfig,
        batch_id: str,
        null_policy: NullPolicyConfig,
    ) -> None:
        lookup = fk.lookup
        assert lookup is not None and fk.references is not None
        if fk.relationship != "type7":
            raise ValueError(
                "Automatic skeletons currently require relationship='type7'"
            )
        if not self.spark.catalog.tableExists(fk.references):
            raise DataQualityError(
                f"Dimension {fk.references} must exist before brokered fact loading"
            )
        dimension = self.spark.table(fk.references)
        required = {
            fk.column,
            fk.durable_column,
            "__valid_from",
            "__valid_to",
            "__is_current",
            "__is_skeleton",
        }
        required.update(lookup.dimension_columns or lookup.source_columns)
        missing_columns = sorted(
            c for c in required if c and c not in dimension.columns
        )
        if missing_columns:
            raise DataQualityError(
                f"Dimension {fk.references} does not satisfy its Type 7 contract: "
                + ", ".join(missing_columns)
            )

        source_columns = lookup.source_columns
        dimension_columns = lookup.dimension_columns or source_columns
        event_time = lookup.event_time
        assert event_time is not None
        usable = fact_df.filter(
            ~_any_null(source_columns) & F.col(event_time).isNotNull()
        )
        for name in source_columns:
            usable = usable.filter(F.trim(F.col(name).cast("string")) != "")
        if lookup.not_applicable_when:
            usable = usable.filter(~F.expr(lookup.not_applicable_when))
        candidates = usable.groupBy(*source_columns).agg(
            F.min(F.col(event_time)).alias("__valid_from")
        )
        candidates = candidates.select(
            *[
                F.col(source).alias(dimension)
                for source, dimension in zip(
                    source_columns, dimension_columns, strict=True
                )
            ],
            "__valid_from",
        )

        dim_keys = dimension.select(
            *dimension_columns, "__valid_from", "__valid_to"
        ).alias("dim")
        candidate = candidates.alias("candidate")
        identity_match = reduce(
            lambda left, right: left & right,
            (
                F.col(f"candidate.`{name}`") == F.col(f"dim.`{name}`")
                for name in dimension_columns
            ),
        )
        covered = (
            identity_match
            & (F.col("candidate.__valid_from") >= F.col("dim.__valid_from"))
            & (F.col("candidate.__valid_from") < F.col("dim.__valid_to"))
        )
        skeletons = candidate.join(dim_keys, covered, "left_anti")

        key_generator = HashKeyGenerator(
            dimension_columns, version_column="__valid_from"
        )
        skeletons = key_generator.generate_type7_keys(
            skeletons,
            fk.column,
            fk.durable_column or "durable_key",
        )
        selected: list[Column] = []
        available = set(skeletons.columns)
        substitutes = null_policy.attribute_substitutes
        for field in dimension.schema.fields:
            name = field.name
            if name in available:
                selected.append(F.col(name).cast(field.dataType).alias(name))
            elif name == "__valid_to":
                selected.append(F.lit(DEFAULT_VALID_TO).cast("timestamp").alias(name))
            elif name == "__is_current":
                selected.append(F.lit(True).alias(name))
            elif name == "__is_skeleton":
                selected.append(F.lit(True).alias(name))
            elif name == "__is_deleted":
                selected.append(F.lit(False).alias(name))
            elif name == "__member_status":
                selected.append(F.lit("NOT_YET_AVAILABLE").alias(name))
            elif name == "__key_origin":
                selected.append(F.lit("skeleton").alias(name))
            elif name == "__etl_batch_id":
                selected.append(F.lit(batch_id).alias(name))
            elif name == "__etl_processed_at" or name == "__skeleton_created_at":
                selected.append(F.current_timestamp().alias(name))
            elif name in substitutes:
                selected.append(
                    F.lit(substitutes[name]).cast(field.dataType).alias(name)
                )
            else:
                selected.append(_placeholder(field).alias(name))
        skeletons = skeletons.select(*selected)

        condition = " AND ".join(
            f"target.`{name}` = source.`{name}`" for name in dimension_columns
        )
        condition += " AND target.__valid_from = source.__valid_from"
        DeltaTable.forName(self.spark, fk.references).alias("target").merge(
            skeletons.alias("source"), condition
        ).whenNotMatchedInsertAll().execute()

    def _resolve_relationship(
        self,
        fact_df: DataFrame,
        fk: ForeignKeyConfig,
        index: int,
        *,
        original_identity_columns: list[str],
        fact_table: str,
        fact_grain: list[str],
        batch_id: str,
        source_version: int,
    ) -> DataFrame:
        lookup = fk.lookup
        assert lookup is not None and fk.references is not None
        dimension = self.spark.table(fk.references)
        source_columns = lookup.source_columns
        dimension_columns = lookup.dimension_columns or source_columns
        required_dimension_columns = {
            *dimension_columns,
            fk.dimension_key or fk.column,
        }
        if fk.durable_dimension_key:
            required_dimension_columns.add(fk.durable_dimension_key)
        if fk.relationship == "type7":
            required_dimension_columns.update({"__valid_from", "__valid_to"})
        missing_dimension_columns = sorted(
            required_dimension_columns - set(dimension.columns)
        )
        if missing_dimension_columns:
            raise DataQualityError(
                f"Dimension {fk.references} is missing broker columns: "
                + ", ".join(missing_dimension_columns)
            )
        prefix = f"__broker_{index}_"
        projected = [
            F.col(name).alias(f"{prefix}identity_{position}")
            for position, name in enumerate(dimension_columns)
        ]
        projected.append(F.col(fk.dimension_key or fk.column).alias(f"{prefix}sk"))
        if fk.durable_dimension_key:
            projected.append(F.col(fk.durable_dimension_key).alias(f"{prefix}dk"))
        if fk.relationship == "type7":
            projected.extend(
                [
                    F.col("__valid_from").alias(f"{prefix}valid_from"),
                    F.col("__valid_to").alias(f"{prefix}valid_to"),
                ]
            )
        dimension = dimension.select(*projected)
        condition = reduce(
            lambda left, right: left & right,
            (
                fact_df[source] == dimension[f"{prefix}identity_{position}"]
                for position, source in enumerate(source_columns)
            ),
        )
        if fk.relationship == "type7":
            assert lookup.event_time is not None
            condition = (
                condition
                & (fact_df[lookup.event_time] >= dimension[f"{prefix}valid_from"])
                & (fact_df[lookup.event_time] < dimension[f"{prefix}valid_to"])
            )
        joined = fact_df.join(dimension, condition, "left")
        not_applicable = (
            F.expr(lookup.not_applicable_when)
            if lookup.not_applicable_when
            else F.lit(False)
        )
        missing_identity = _any_null(source_columns)
        bad_value = F.lit(False)
        for name in source_columns:
            bad_value = bad_value | (
                F.col(name).isNotNull() & (F.trim(F.col(name).cast("string")) == "")
            )
        if lookup.event_time:
            bad_value = bad_value | F.col(lookup.event_time).isNull()
        unmatched = F.col(f"{prefix}sk").isNull()
        special_key = (
            F.when(not_applicable, F.lit(-2))
            .when(missing_identity, F.lit(-1))
            .when(bad_value, F.lit(-4))
            .when(unmatched, F.lit(-3))
            .otherwise(F.col(f"{prefix}sk"))
        )
        joined = joined.withColumn(fk.column, special_key.cast("bigint"))
        if fk.durable_column:
            durable = (
                F.when(not_applicable, F.lit(-2))
                .when(missing_identity, F.lit(-1))
                .when(bad_value, F.lit(-4))
                .when(F.col(f"{prefix}dk").isNull(), F.lit(-3))
                .otherwise(F.col(f"{prefix}dk"))
            )
            joined = joined.withColumn(fk.durable_column, durable.cast("bigint"))

        if lookup.invalid_action == "error":
            invalid = joined.filter(
                ~not_applicable & ~missing_identity & bad_value
            ).limit(1)
            if invalid.collect():
                raise DataQualityError(
                    f"Key broker received an invalid identity/event time for {fk.column}"
                )

        unresolved_condition = (
            ~not_applicable & ~missing_identity & ~bad_value & unmatched
        )
        if lookup.early_arriving in {"skeleton", "error"}:
            unresolved = joined.filter(unresolved_condition).limit(1)
            if unresolved.collect():
                raise DataQualityError(
                    f"Key broker could not resolve {fk.column} against {fk.references}"
                )
        if (
            lookup.early_arriving == "default"
            and self.unresolved_registry is not None
            and fact_table
            and fact_grain
        ):
            dimension_version = self._table_version(fk.references)
            unresolved_rows = joined.filter(unresolved_condition)
            identity_columns = original_identity_columns or source_columns
            self.unresolved_registry.record(
                unresolved_rows,
                fact_table=fact_table,
                relationship=fk.column,
                fact_grain=fact_grain,
                source_identity=identity_columns,
                event_time=lookup.event_time,
                batch_id=batch_id,
                source_version=source_version,
                dimension_version=dimension_version,
            )
            self.unresolved_registry.resolve(
                joined.filter(
                    ~not_applicable & ~missing_identity & ~bad_value & ~unmatched
                ),
                fact_table=fact_table,
                relationship=fk.column,
                fact_grain=fact_grain,
                batch_id=batch_id,
                dimension_version=dimension_version,
            )
        internal_prefixes = (prefix, f"__identity_{index}_")
        return joined.drop(
            *[
                name
                for name in joined.columns
                if any(name.startswith(value) for value in internal_prefixes)
            ]
        )

    def _table_version(self, table_name: str) -> int:
        try:
            row = (
                self.spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
                .select("version")
                .first()
            )
            return int(row["version"]) if row is not None else -1
        except Exception:
            return -1
