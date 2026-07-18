from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from kimball.common.config import ForeignKeyConfig, NullPolicyConfig
from kimball.common.constants import DEFAULT_VALID_TO
from kimball.common.errors import DataQualityError
from kimball.observability.unresolved_keys import UnresolvedKeyRegistry
from kimball.processing.dimension_nulls import apply_dimension_null_policy
from kimball.processing.key_broker import KeyBroker
from kimball.processing.key_generator import HashKeyGenerator
from kimball.processing.key_integrity import validate_type7_keys
from kimball.processing.scd2 import merge_scd2


def _dimension_rows(spark: SparkSession, rows: list[tuple]):
    frame = spark.createDataFrame(
        rows,
        "customer_id string, name string, updated_at timestamp, "
        "__valid_from timestamp, __valid_to timestamp, __is_current boolean",
    )
    keyed = HashKeyGenerator(
        ["customer_id"], version_column="updated_at"
    ).generate_type7_keys(frame, "customer_sk", "customer_dk")
    return (
        keyed.withColumn("hashdiff", F.lit(-1).cast("long"))
        .withColumn("__is_deleted", F.lit(False))
        .withColumn("__is_skeleton", F.lit(False))
        .withColumn("__member_status", F.lit("REAL"))
        .withColumn("__key_origin", F.lit("generated"))
        .withColumn("__etl_processed_at", F.current_timestamp())
        .withColumn("__etl_batch_id", F.lit("setup"))
    )


def _type7_fk(table: str, **lookup_overrides) -> ForeignKeyConfig:
    lookup = {
        "source_columns": ["customer_id"],
        "event_time": "order_at",
        "early_arriving": "default",
        "invalid_action": "default",
        **lookup_overrides,
    }
    return ForeignKeyConfig(
        column="customer_sk",
        references=table,
        dimension_key="customer_sk",
        relationship="type7",
        durable_column="customer_dk",
        durable_dimension_key="customer_dk",
        lookup=lookup,
    )


def test_broker_assigns_dual_keys_and_all_reserved_states(spark, test_db) -> None:
    dimension_table = f"{test_db}.dim_customer"
    registry = UnresolvedKeyRegistry(spark, test_db)
    _dimension_rows(
        spark,
        [
            (
                "A",
                "Old A",
                datetime(2024, 1, 1),
                datetime(2024, 1, 1),
                datetime(2025, 1, 1),
                False,
            ),
            (
                "A",
                "Current A",
                datetime(2025, 1, 1),
                datetime(2025, 1, 1),
                DEFAULT_VALID_TO,
                True,
            ),
        ],
    ).write.format("delta").saveAsTable(dimension_table)
    facts = spark.createDataFrame(
        [
            (1, "A", datetime(2024, 6, 1), False),
            (2, "A", datetime(2025, 6, 1), False),
            (3, None, datetime(2025, 6, 1), False),
            (4, "N/A", datetime(2025, 6, 1), True),
            (5, "Z", datetime(2025, 6, 1), False),
            (6, "", datetime(2025, 6, 1), False),
        ],
        "order_id int, customer_id string, order_at timestamp, not_applicable boolean",
    )
    fk = _type7_fk(dimension_table, not_applicable_when="not_applicable")
    resolved = KeyBroker(spark, registry).resolve_fact_keys(
        facts,
        [fk],
        batch_id="batch-1",
        null_policy=NullPolicyConfig(),
        fact_table=f"{test_db}.fact_sales",
        fact_grain=["order_id"],
        source_version=12,
    )
    actual = {
        row["order_id"]: (row["customer_sk"], row["customer_dk"])
        for row in resolved.collect()
    }
    assert actual[1][0] != actual[2][0]
    assert actual[1][1] == actual[2][1]
    assert actual[3] == (-1, -1)
    assert actual[4] == (-2, -2)
    assert actual[5] == (-3, -3)
    assert actual[6] == (-4, -4)
    evidence = spark.table(registry.table_name).collect()
    assert len(evidence) == 1
    assert evidence[0]["status"] == "OPEN"
    assert evidence[0]["source_version"] == 12
    _dimension_rows(
        spark,
        [
            (
                "Z",
                "Arrived Z",
                datetime(2025, 1, 1),
                datetime(2025, 1, 1),
                DEFAULT_VALID_TO,
                True,
            )
        ],
    ).write.format("delta").mode("append").saveAsTable(dimension_table)
    replay = (
        KeyBroker(spark, registry)
        .resolve_fact_keys(
            facts.filter("order_id = 5"),
            [fk],
            batch_id="batch-2",
            null_policy=NullPolicyConfig(),
            fact_table=f"{test_db}.fact_sales",
            fact_grain=["order_id"],
            source_version=13,
        )
        .first()
    )
    assert replay["customer_sk"] not in {-1, -2, -3, -4}
    assert spark.table(registry.table_name).first()["status"] == "RESOLVED"


def test_identity_map_resolves_supplier_identity_without_rewriting_fact(
    spark, test_db
) -> None:
    dimension_table = f"{test_db}.dim_customer"
    identity_table = f"{test_db}.customer_identity_map"
    _dimension_rows(
        spark,
        [
            (
                "A",
                "Survivor",
                datetime(2024, 1, 1),
                datetime(2024, 1, 1),
                DEFAULT_VALID_TO,
                True,
            )
        ],
    ).write.format("delta").saveAsTable(dimension_table)
    spark.createDataFrame(
        [
            ("A", "A", datetime(2020, 1, 1), DEFAULT_VALID_TO),
            ("B", "A", datetime(2025, 1, 1), DEFAULT_VALID_TO),
        ],
        "source_identity string, canonical_identity string, "
        "valid_from timestamp, valid_to timestamp",
    ).write.format("delta").saveAsTable(identity_table)
    facts = spark.createDataFrame(
        [(1, "B", datetime(2026, 1, 1))],
        "order_id int, customer_id string, order_at timestamp",
    )
    resolved = (
        KeyBroker(spark)
        .resolve_fact_keys(
            facts,
            [
                _type7_fk(
                    dimension_table, identity_map=identity_table, early_arriving="error"
                )
            ],
            batch_id="batch-1",
            null_policy=NullPolicyConfig(),
        )
        .first()
    )
    expected = spark.table(dimension_table).first()
    assert resolved["customer_sk"] == expected["customer_sk"]
    assert resolved["customer_dk"] == expected["customer_dk"]
    assert resolved["customer_id"] == "A"


def test_skeleton_key_is_preserved_when_real_member_hydrates(spark, test_db) -> None:
    dimension_table = f"{test_db}.dim_customer"
    empty = _dimension_rows(
        spark,
        [
            (
                "seed",
                "seed",
                datetime(2020, 1, 1),
                datetime(2020, 1, 1),
                DEFAULT_VALID_TO,
                True,
            )
        ],
    ).limit(0)
    empty.write.format("delta").saveAsTable(dimension_table)
    facts = spark.createDataFrame(
        [(1, "LATE", datetime(2025, 6, 1))],
        "order_id int, customer_id string, order_at timestamp",
    )
    fk = _type7_fk(dimension_table, early_arriving="skeleton")
    first = (
        KeyBroker(spark)
        .resolve_fact_keys(
            facts,
            [fk],
            batch_id="batch-1",
            null_policy=NullPolicyConfig(),
        )
        .first()
    )
    skeleton = spark.table(dimension_table).filter("customer_id = 'LATE'").first()
    assert first["customer_sk"] == skeleton["customer_sk"]
    assert skeleton["__is_skeleton"] is True
    merge_scd2(
        spark.createDataFrame(
            [("LATE", "Real Name", datetime(2025, 5, 1))],
            "customer_id string, name string, updated_at timestamp",
        ),
        target_table_name=dimension_table,
        join_keys=["customer_id"],
        track_history_columns=["name"],
        surrogate_key_col="customer_sk",
        effective_at_column="updated_at",
        durable_key_col="customer_dk",
        scd_type=7,
    )
    hydrated = spark.table(dimension_table).filter("customer_id = 'LATE'").first()
    assert hydrated["customer_sk"] == skeleton["customer_sk"]
    assert hydrated["__valid_from"] == skeleton["__valid_from"]
    assert hydrated["__is_skeleton"] is False
    assert hydrated["name"] == "Real Name"


def test_type7_collision_evidence_fails_before_target_mutation(spark) -> None:
    source = spark.createDataFrame(
        [
            (42, 100, "row-a", "durable-a"),
            (42, 101, "row-b", "durable-b"),
        ],
        "customer_sk long, customer_dk long, "
        "__row_key_fingerprint string, __durable_key_fingerprint string",
    )
    target = source.limit(0)

    with pytest.raises(DataQualityError, match="collision"):
        validate_type7_keys(
            source,
            target,
            surrogate_key="customer_sk",
            durable_key="customer_dk",
        )


def test_type7_integrity_allows_preseeded_reserved_target_members(spark) -> None:
    source = spark.createDataFrame(
        [(42, 100, "row-a", "durable-a")],
        "customer_sk long, customer_dk long, "
        "__row_key_fingerprint string, __durable_key_fingerprint string",
    )
    target = spark.createDataFrame(
        [(-3, -3, "reserved-row", "reserved-durable")],
        source.schema,
    )

    validate_type7_keys(
        source,
        target,
        surrogate_key="customer_sk",
        durable_key="customer_dk",
    )


def test_kimball_null_policy_substitutes_attributes_but_rejects_null_identity(
    spark,
) -> None:
    source = spark.createDataFrame(
        [("A", None, datetime(2025, 1, 1))],
        "customer_id string, city string, updated_at timestamp",
    )
    resolved = apply_dimension_null_policy(
        source,
        NullPolicyConfig(),
        identity_columns=["customer_id", "updated_at"],
    ).first()
    assert resolved["city"] == "Missing"

    invalid = spark.createDataFrame(
        [(None, "Lisbon", datetime(2025, 1, 1))],
        "customer_id string, city string, updated_at timestamp",
    )
    with pytest.raises(DataQualityError, match="must not contain NULL"):
        apply_dimension_null_policy(
            invalid,
            NullPolicyConfig(),
            identity_columns=["customer_id", "updated_at"],
        )
