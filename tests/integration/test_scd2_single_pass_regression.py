"""Regression tests for the two critical bugs in ``_merge_single_pass``.

These cover the canonical SCD2 single-pass path used for snapshots and CDF
batches of every size. The bugs covered here were silent data-corruption
issues; the tests pin the fixed behaviour and fail on the pre-fix code.

Bug 1 -- skeleton hydration
    When a multi-version batch targets an early-arriving-fact skeleton, the
    skeleton must be hydrated IN PLACE: it keeps its original surrogate key and
    takes the latest version's attributes. Before the fix the skeleton was left
    unfilled and a null-SK duplicate was inserted, orphaning fact FKs that
    already pointed at the skeleton's SK.

Bug 2 -- expire ``__valid_to`` overlap
    When >=2 new versions of a key arrive, the old current target row must be
    expired at ``(oldest_new_valid_from - 1us)``, not
    ``(latest_new_valid_from - 1us)``. The old behaviour made the expired row's
    validity interval overlap the back-filled intermediate versions, so a
    point-in-time read between the oldest and latest new version returned the
    stale old row instead of the correct intermediate version.

The tests call the public ``merge_scd2`` entry point against real Delta tables
so the single-pass dispatch and the MERGE are both exercised end-to-end. They
require a real Spark + Delta session (local or Databricks); they are not
mock-based.
"""

from __future__ import annotations

import uuid

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from kimball.processing.scd2 import merge_scd2

pytestmark = pytest.mark.usefixtures("spark")

TABLE = "dim_customer_reg"

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


@pytest.fixture
def scd2_db(spark: SparkSession) -> str:
    """A throwaway database per test, dropped on teardown."""
    db = f"kimball_scd2_reg_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    yield db
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")


def _create_target(spark: SparkSession, db: str) -> None:
    spark.sql(f"""
        CREATE TABLE {db}.{TABLE} (
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


def _make_source(spark: SparkSession, rows: list[dict]) -> DataFrame:
    prepared: list[dict] = []
    for i, r in enumerate(rows):
        row = dict(r)
        row.setdefault("_change_type", "insert")
        row.setdefault("_commit_version", i + 1)
        prepared.append(row)
    return spark.createDataFrame(prepared, CUSTOMER_SCHEMA)


def _run(spark: SparkSession, db: str, source: DataFrame) -> None:
    merge_scd2(
        source,
        target_table_name=f"{db}.{TABLE}",
        join_keys=["customer_id"],
        track_history_columns=["name", "email"],
        surrogate_key_col="customer_sk",
        effective_at_column="updated_at",
        schema_evolution=False,
    )


class TestIncrementalCdfDeleteSafety:
    """An incremental CDF update must not expire keys absent from that commit."""

    def test_update_without_delete_keeps_untouched_current_rows(
        self, spark: SparkSession, scd2_db: str
    ) -> None:
        _create_target(spark, scd2_db)
        initial = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Ada",
                    "email": "ada@old",
                    "updated_at": "2024-01-01",
                },
                {
                    "customer_id": 2,
                    "name": "Bea",
                    "email": "bea@old",
                    "updated_at": "2024-01-01",
                },
                {
                    "customer_id": 3,
                    "name": "Cy",
                    "email": "cy@old",
                    "updated_at": "2024-01-01",
                },
            ],
        )
        # Initial source is a snapshot, so it intentionally has no CDF marker.
        _run(spark, scd2_db, initial.drop("_change_type", "_commit_version"))

        # A later CDF commit updates customer 1 only and contains no deletes.
        incremental = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Ada",
                    "email": "ada@new",
                    "updated_at": "2024-02-01",
                    "_change_type": "update_postimage",
                    "_commit_version": 10,
                }
            ],
        )
        _run(spark, scd2_db, incremental)

        current = {
            r.customer_id: r
            for r in spark.table(f"{scd2_db}.{TABLE}")
            .filter("__is_current = true")
            .collect()
        }
        assert set(current) == {1, 2, 3}
        assert current[2]["__is_deleted"] is False
        assert current[3]["__is_deleted"] is False

    def test_delete_only_commit_expires_only_the_deleted_key(
        self, spark: SparkSession, scd2_db: str
    ) -> None:
        _create_target(spark, scd2_db)
        initial = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Ada",
                    "email": "ada@x",
                    "updated_at": "2024-01-01",
                },
                {
                    "customer_id": 2,
                    "name": "Bea",
                    "email": "bea@x",
                    "updated_at": "2024-01-01",
                },
                {
                    "customer_id": 3,
                    "name": "Cy",
                    "email": "cy@x",
                    "updated_at": "2024-01-01",
                },
            ],
        )
        _run(spark, scd2_db, initial.drop("_change_type", "_commit_version"))

        _run(
            spark,
            scd2_db,
            _make_source(
                spark,
                [
                    {
                        "customer_id": 2,
                        "name": "Bea",
                        "email": "bea@x",
                        "updated_at": "2024-02-01",
                        "_change_type": "delete",
                        "_commit_version": 10,
                    }
                ],
            ),
        )

        current_keys = {
            row.customer_id
            for row in spark.table(f"{scd2_db}.{TABLE}")
            .filter("__is_current = true")
            .collect()
        }
        deleted = (
            spark.table(f"{scd2_db}.{TABLE}")
            .filter("customer_id = 2 AND __is_deleted = true")
            .collect()
        )
        assert current_keys == {1, 3}
        assert len(deleted) == 1


# =====================================================================
# Bug 1: skeleton hydration
# =====================================================================


class TestSinglePassSkeletonHydration:
    """A skeleton targeted by a multi-version batch is hydrated in place."""

    def test_skeleton_hydrated_in_place_no_null_sk_duplicate(
        self, spark: SparkSession, scd2_db: str
    ):
        _create_target(spark, scd2_db)
        # Pre-seed a skeleton: real SK 1001, placeholder attributes, current.
        spark.sql(f"""
            INSERT INTO {scd2_db}.{TABLE}
            SELECT 1001, 1, NULL, NULL, 0, true,
                   '1900-01-01'::timestamp, NULL, false, true, current_timestamp()
        """)

        # Two versions of customer 1 in one batch -> forces _merge_single_pass.
        source = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Alice",
                    "email": "alice@x.com",
                    "updated_at": "2024-01-01",
                },
                {
                    "customer_id": 1,
                    "name": "Alice",
                    "email": "alice@y.com",
                    "updated_at": "2024-06-01",
                },
            ],
        )
        _run(spark, scd2_db, source)

        rows = spark.sql(f"""
            SELECT customer_sk, customer_id, name, email, __is_current, __is_skeleton
            FROM {scd2_db}.{TABLE}
            WHERE customer_id = 1
        """).collect()

        # Exactly one current row, hydrated with the LATEST version's attributes,
        # keeping the skeleton's original SK (1001).
        current = [r for r in rows if r["__is_current"]]
        assert len(current) == 1, (
            f"expected one current row, got {len(current)}: {rows}"
        )
        assert current[0]["customer_sk"] == 1001, (
            f"skeleton SK must be preserved on hydration; got {current[0]['customer_sk']}"
        )
        assert current[0]["__is_skeleton"] is False
        assert current[0]["name"] == "Alice"
        assert current[0]["email"] == "alice@y.com"

        # No null-SK duplicate (the original bug inserted one).
        null_sk = spark.sql(
            f"SELECT COUNT(*) AS c FROM {scd2_db}.{TABLE} WHERE customer_sk IS NULL"
        ).first()["c"]
        assert null_sk == 0, (
            f"found {null_sk} null-SK row(s) -- skeleton not hydrated in place"
        )

    def test_fact_fk_resolves_to_hydrated_attributes(
        self, spark: SparkSession, scd2_db: str
    ):
        """A fact FK keyed to the skeleton's SK must resolve to real attrs after hydration."""
        _create_target(spark, scd2_db)
        spark.sql(f"""
            INSERT INTO {scd2_db}.{TABLE}
            SELECT 1001, 1, NULL, NULL, 0, true,
                   '1900-01-01'::timestamp, NULL, false, true, current_timestamp()
        """)
        source = _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Alice",
                    "email": "alice@x.com",
                    "updated_at": "2024-01-01",
                },
                {
                    "customer_id": 1,
                    "name": "Alice",
                    "email": "alice@y.com",
                    "updated_at": "2024-06-01",
                },
            ],
        )
        _run(spark, scd2_db, source)

        # A fact that arrived early and keyed to the skeleton's SK.
        fact = spark.sql("""
            SELECT 1001 AS customer_sk, 42 AS amount, '2024-03-01'::timestamp AS ts
        """)
        resolved = (
            fact.join(spark.table(f"{scd2_db}.{TABLE}"), "customer_sk", "left")
            .filter("__is_current = true")
            .select("name", "email")
            .collect()
        )
        assert len(resolved) == 1, (
            f"fact FK did not resolve to exactly one current dim row: {resolved}"
        )
        assert resolved[0]["name"] == "Alice"
        assert resolved[0]["email"] == "alice@y.com", (
            f"FK resolved to stale/placeholder attrs: {resolved[0]}"
        )


# =====================================================================
# Bug 2: expire __valid_to overlap
# =====================================================================


class TestSinglePassExpireValidTo:
    """The old current row is expired at oldest_new_valid_from - 1us."""

    def _seed_old_current(self, spark: SparkSession, db: str) -> None:
        _create_target(spark, db)
        spark.sql(f"""
            INSERT INTO {db}.{TABLE}
            SELECT 5001, 1, 'Alice', 'alice@old.com', 0, true,
                   '2023-01-01'::timestamp, NULL, false, false, current_timestamp()
        """)

    def _three_version_batch(self, spark: SparkSession) -> DataFrame:
        # oldest v1 = 2024-03-01, middle v2 = 2024-06-01, latest v3 = 2024-09-01
        return _make_source(
            spark,
            [
                {
                    "customer_id": 1,
                    "name": "Alice",
                    "email": "alice@a.com",
                    "updated_at": "2024-03-01",
                },
                {
                    "customer_id": 1,
                    "name": "Alice",
                    "email": "alice@b.com",
                    "updated_at": "2024-06-01",
                },
                {
                    "customer_id": 1,
                    "name": "Alice",
                    "email": "alice@c.com",
                    "updated_at": "2024-09-01",
                },
            ],
        )

    def test_old_row_expired_at_oldest_new_version(
        self, spark: SparkSession, scd2_db: str
    ):
        self._seed_old_current(spark, scd2_db)
        _run(spark, scd2_db, self._three_version_batch(spark))

        old_row = spark.sql(f"""
            SELECT __valid_to, __is_current, __is_deleted
            FROM {scd2_db}.{TABLE}
            WHERE email = 'alice@old.com'
        """).first()
        assert old_row is not None
        assert old_row["__is_current"] is False
        assert old_row["__is_deleted"] is False

        # Must be expired at oldest_new_valid_from - 1us (2024-02-29 23:59:59.999999),
        # NOT at latest_new_valid_from - 1us (2024-08-31 23:59:59.999999).
        expected = spark.sql(
            "SELECT '2024-03-01'::timestamp - INTERVAL 1 MICROSECOND AS t"
        ).first()["t"]
        assert old_row["__valid_to"] == expected, (
            f"old row __valid_to should be oldest_new - 1us ({expected}); "
            f"got {old_row['__valid_to']} (overlap bug if 2024-08-31 23:59:59.999999)"
        )

    def test_point_in_time_read_between_versions_returns_intermediate(
        self, spark: SparkSession, scd2_db: str
    ):
        """A PIT read at 2024-04-15 (between v1 and v2) returns v1, not the stale old row."""
        self._seed_old_current(spark, scd2_db)
        _run(spark, scd2_db, self._three_version_batch(spark))

        # Before the fix the old row (alice@old.com) stayed valid through
        # 2024-08-31 and was returned here, masking the correct intermediate.
        pit = spark.sql(f"""
            SELECT email
            FROM {scd2_db}.{TABLE}
            WHERE customer_id = 1
              AND __valid_from <= '2024-04-15'::timestamp
              AND (__valid_to > '2024-04-15'::timestamp OR __is_current = true)
        """).collect()
        emails = sorted(r["email"] for r in pit)
        assert emails == ["alice@a.com"], (
            f"PIT read at 2024-04-15 should return only alice@a.com; got {emails} "
            "(stale old row overlapping the back-filled versions)"
        )

    def test_point_in_time_read_before_latest_returns_second_version(
        self, spark: SparkSession, scd2_db: str
    ) -> None:
        self._seed_old_current(spark, scd2_db)
        _run(spark, scd2_db, self._three_version_batch(spark))

        pit = spark.sql(f"""
            SELECT email
            FROM {scd2_db}.{TABLE}
            WHERE customer_id = 1
              AND __valid_from <= '2024-07-15'::timestamp
              AND (__valid_to > '2024-07-15'::timestamp OR __is_current = true)
        """).collect()
        assert [row["email"] for row in pit] == ["alice@b.com"]
