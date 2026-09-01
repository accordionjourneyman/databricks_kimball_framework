"""Behavior tests for the single-pass SCD2 merge and the key broker.

Brings the framework's two riskiest data-plane modules into unit-scope
coverage (CI measures tests/unit only; the golden/integration suites run
in a separate workflow). Same proven pattern as the regression suite:
per-test throwaway Delta databases, real merges, content assertions.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from kimball.processing.key_broker import KeyBroker
from kimball.processing.scd2 import merge_scd2

pytestmark = pytest.mark.usefixtures("spark")

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("updated_at", StringType(), False),
        StructField("_change_type", StringType(), True),
        StructField("_commit_version", IntegerType(), True),
    ]
)

FK_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
    ]
)


@pytest.fixture
def scd2_db(spark) -> Iterator[str]:
    """A throwaway database per test, dropped on teardown."""
    db = f"kimball_scd2_unit_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    yield db
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")


def _create_target(spark, db: str, table: str = "dim_customer") -> None:
    spark.sql(f"""
        CREATE TABLE {db}.{table} (
            customer_sk BIGINT,
            customer_id INT,
            name STRING,
            email STRING,
            hashdiff BIGINT,
            __is_current BOOLEAN,
            __valid_from TIMESTAMP,
            __valid_to TIMESTAMP,
            __is_deleted BOOLEAN,
            __is_skeleton BOOLEAN,
            __etl_processed_at TIMESTAMP
        ) USING DELTA
    """)


def _make_source(spark, rows: list[dict]):
    prepared = []
    for i, r in enumerate(rows):
        row = dict(r)
        row.setdefault("_change_type", "insert")
        row.setdefault("_commit_version", i + 1)
        prepared.append(row)
    return spark.createDataFrame(prepared, CUSTOMER_SCHEMA)


def _run(spark, db: str, source, table: str = "dim_customer", **overrides) -> None:
    merge_scd2(
        source,
        target_table_name=f"{db}.{table}",
        join_keys=["customer_id"],
        track_history_columns=["name", "email"],
        surrogate_key_col="customer_sk",
        effective_at_column="updated_at",
        schema_evolution=False,
        **overrides,
    )


class TestMergeScd2SinglePass:
    def test_initial_load_inserts_current_rows(self, spark, scd2_db):
        _create_target(spark, scd2_db)
        source = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Ada",
                    "email": "a@x",
                    "updated_at": "2024-01-01",
                },
                {
                    "customer_id": 2,
                    "name": "Bea",
                    "email": "b@x",
                    "updated_at": "2024-01-01",
                },
            ],
        ).drop("_change_type", "_commit_version")
        _run(spark, scd2_db, source)
        rows = {
            (r["customer_id"], r["__is_current"]): r
            for r in spark.table(f"{scd2_db}.dim_customer").collect()
        }
        assert (1, True) in rows and (2, True) in rows
        assert rows[(1, True)]["customer_sk"] is not None

    def test_update_expires_old_and_inserts_new_version(self, spark, scd2_db):
        _create_target(spark, scd2_db)
        initial = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Ada",
                    "email": "a@old",
                    "updated_at": "2024-01-01",
                }
            ],
        ).drop("_change_type", "_commit_version")
        _run(spark, scd2_db, initial)

        update = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Ada",
                    "email": "a@new",
                    "updated_at": "2024-06-01",
                }
            ],
        )
        _run(spark, scd2_db, update)

        rows = [r for r in spark.table(f"{scd2_db}.dim_customer").collect()]
        current = [r for r in rows if r["__is_current"]]
        expired = [r for r in rows if not r["__is_current"]]
        assert len(current) == 1 and current[0]["email"] == "a@new"
        assert len(expired) == 1 and expired[0]["email"] == "a@old"

    def test_empty_source_is_noop(self, spark, scd2_db):
        _create_target(spark, scd2_db)
        initial = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Ada",
                    "email": "a@x",
                    "updated_at": "2024-01-01",
                }
            ],
        ).drop("_change_type", "_commit_version")
        _run(spark, scd2_db, initial)
        empty = _make_source(spark, []).drop("_change_type", "_commit_version")
        _run(spark, scd2_db, empty)  # must not raise
        assert spark.table(f"{scd2_db}.dim_customer").count() == 1

    def test_missing_track_columns_raise(self, spark, scd2_db):
        with pytest.raises(ValueError, match="track_history_columns"):
            merge_scd2(
                _make_source(spark, []),
                target_table_name=f"{scd2_db}.nope",
                join_keys=["customer_id"],
                track_history_columns=[],
                surrogate_key_col="customer_sk",
            )

    def test_cdf_delete_expires_row(self, spark, scd2_db):
        _create_target(spark, scd2_db)
        initial = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Ada",
                    "email": "a@x",
                    "updated_at": "2024-01-01",
                },
                {
                    "customer_id": 2,
                    "name": "Bea",
                    "email": "b@x",
                    "updated_at": "2024-01-01",
                },
            ],
        ).drop("_change_type", "_commit_version")
        _run(spark, scd2_db, initial)

        deletes = _make_source(
            spark,
            [
                {
                    "customer_id": 2,
                    "name": "Bea",
                    "email": "b@x",
                    "updated_at": "2024-06-01",
                    "_change_type": "delete",
                },
            ],
        )
        _run(spark, scd2_db, deletes)
        rows = [r for r in spark.table(f"{scd2_db}.dim_customer").collect()]
        current = [r for r in rows if r["__is_current"]]
        assert {r["customer_id"] for r in current} == {1}


class TestKeyBrokerResolution:
    """KeyBroker set-based FK resolution against a real SCD2 dimension."""

    def _brokered_fact(self, spark, scd2_db: str, fact_rows: list[tuple]):
        from kimball.common.config import (
            ForeignKeyConfig,
            ForeignKeyLookupConfig,
            NullPolicyConfig,
        )

        fact_schema = StructType(
            [
                StructField("order_id", IntegerType(), False),
                StructField("customer_id", IntegerType(), True),
            ]
        )
        fact = spark.createDataFrame(fact_rows, fact_schema)
        fk = ForeignKeyConfig(
            column="customer_sk",
            references=f"{scd2_db}.dim_customer",
            dimension_key="customer_id",
            lookup=ForeignKeyLookupConfig(
                source_columns=["customer_id"],
                early_arriving="default",
            ),
        )
        broker = KeyBroker(spark)
        return broker.resolve_fact_keys(
            fact,
            [fk],
            batch_id="unit-batch",
            null_policy=NullPolicyConfig(),
            fact_table=f"{scd2_db}.fact_orders",
            fact_grain=["order_id"],
        )

    def test_resolves_present_keys(self, spark, scd2_db):
        _create_target(spark, scd2_db)
        dim_source = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Ada",
                    "email": "a@x",
                    "updated_at": "2024-01-01",
                },
                {
                    "customer_id": 2,
                    "name": "Bea",
                    "email": "b@x",
                    "updated_at": "2024-01-01",
                },
            ],
        ).drop("_change_type", "_commit_version")
        _run(spark, scd2_db, dim_source)

        resolved = self._brokered_fact(spark, scd2_db, [(100, 1), (101, 2)])
        rows = {r["order_id"]: r for r in resolved.collect()}
        assert rows[100]["customer_sk"] is not None
        assert rows[101]["customer_sk"] is not None
        assert rows[100]["customer_sk"] != rows[101]["customer_sk"]

    def test_unknown_key_gets_sentinel(self, spark, scd2_db):
        _create_target(spark, scd2_db)
        dim_source = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Ada",
                    "email": "a@x",
                    "updated_at": "2024-01-01",
                }
            ],
        ).drop("_change_type", "_commit_version")
        _run(spark, scd2_db, dim_source)

        resolved = self._brokered_fact(spark, scd2_db, [(100, 1), (999, 77)])
        rows = {r["order_id"]: r for r in resolved.collect()}
        # Known key resolves; unknown key falls to a reserved sentinel SK.
        assert rows[100]["customer_sk"] is not None
        assert rows[999]["customer_sk"] is not None
        assert rows[999]["customer_sk"] < 0
