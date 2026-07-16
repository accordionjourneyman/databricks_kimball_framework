"""Integration tests for Identity Bridge key resolution.

These tests exercise the REAL bridge resolution logic
(``SkeletonManager.apply_identity_bridge``) on real Spark DataFrames backed
by real catalog tables -- no orchestrator internals are mocked. They verify
the behavior the pipeline now relies on: the ``join_on`` natural-key column is
replaced by the canonical ``target_column`` value when a mapping exists, and
left untouched when it does not.

A final end-to-end test runs the full ``Orchestrator.run()`` pipeline to guard
the wiring in ``TransformValidator.transform_and_validate``: if the bridge call
is ever removed from the pipeline, the dimension would be keyed by the
unresolved alternate key and that test would fail.
"""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from kimball.common.config import IdentityBridgeConfig, TableConfig
from kimball.orchestration.orchestrator import Orchestrator
from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.services.skeleton_manager import SkeletonManager

pytestmark = pytest.mark.usefixtures("spark")


def _ctx(spark: SparkSession, bridge: IdentityBridgeConfig) -> PipelineContext:
    """A minimal PipelineContext carrying only what apply_identity_bridge reads.

    apply_identity_bridge touches ctx.spark and ctx.config.identity_bridge only,
    so the remaining service fields are filled with dummies -- they are NOT the
    code under test and are never exercised here.
    """
    config = TableConfig(
        table_name="dim_bridge_test",
        table_type="dimension",
        surrogate_key="sk",
        natural_keys=["business_key"],
        sources=[],
    )
    config.identity_bridge = bridge
    return PipelineContext(
        spark=spark,
        config=config,
        etl_control=MagicMock(),
        loader=MagicMock(),
        runtime_options=MagicMock(),
    )


class TestIdentityBridgeResolution:
    """Verify apply_identity_bridge resolves keys on real Spark DataFrames."""

    def test_resolves_keys_and_preserves_other_columns(
        self, spark: SparkSession, test_db: str
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.bridge (
                business_key STRING, target_key STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.bridge VALUES
            ('A', 'C'), ('B', 'C'), ('D', 'E')
        """)

        bridge = IdentityBridgeConfig(
            table=f"{test_db}.bridge",
            join_on="business_key",
            target_column="target_key",
        )
        source = spark.createDataFrame(
            [("A", 100), ("B", 200), ("D", 300)], "business_key string, val int"
        )

        resolved = SkeletonManager().apply_identity_bridge(_ctx(spark, bridge), source)
        rows = {r.business_key: r.val for r in resolved.collect()}

        # Both A and B resolve to the SAME canonical key C -- assert each one
        # landed on C (not "one of them might have"), and that the non-key
        # column traveled with its row rather than being collapsed/lost.
        assert rows.get("C") is not None, f"A should resolve to C, got {rows}"
        assert "A" not in rows and "B" not in rows, (
            f"Unresolved alternate keys leaked through: {rows}"
        )
        assert rows["E"] == 300, f"D should resolve to E, got {rows}"
        # The vals 100 and 200 both survive (one under C); collect them to prove
        # no source row was silently dropped during resolution.
        vals_for_c = {
            r.val for r in resolved.filter("business_key = 'C'").collect()
        }
        assert vals_for_c == {100, 200}, (
            f"Both source rows should resolve to C keeping their vals, got {vals_for_c}"
        )

        spark.sql(f"DROP TABLE IF EXISTS {test_db}.bridge")

    def test_preserves_unmapped_keys(self, spark: SparkSession, test_db: str):
        spark.sql(f"""
            CREATE TABLE {test_db}.bridge (
                business_key STRING, target_key STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.bridge VALUES ('A', 'C')
        """)

        bridge = IdentityBridgeConfig(
            table=f"{test_db}.bridge",
            join_on="business_key",
            target_column="target_key",
        )
        # X has no mapping in the bridge -> must pass through unchanged.
        source = spark.createDataFrame(
            [("X", 300)], "business_key string, val int"
        )

        resolved = SkeletonManager().apply_identity_bridge(_ctx(spark, bridge), source)
        rows = resolved.collect()

        assert len(rows) == 1, f"Unmapped key should not multiply, got {rows}"
        assert rows[0].business_key == "X", (
            f"Unmapped key should pass through as X, got {rows[0].business_key}"
        )
        assert rows[0].val == 300

        spark.sql(f"DROP TABLE IF EXISTS {test_db}.bridge")

    def test_handles_empty_bridge_table(self, spark: SparkSession, test_db: str):
        spark.sql(f"""
            CREATE TABLE {test_db}.bridge (
                business_key STRING, target_key STRING
            ) USING DELTA
        """)
        # No rows inserted -- empty bridge.

        bridge = IdentityBridgeConfig(
            table=f"{test_db}.bridge",
            join_on="business_key",
            target_column="target_key",
        )
        source = spark.createDataFrame(
            [("A", 100), ("B", 200)], "business_key string, val int"
        )

        resolved = SkeletonManager().apply_identity_bridge(_ctx(spark, bridge), source)
        rows = resolved.collect()

        # With no mappings, every key is unchanged.
        assert len(rows) == 2, f"Empty bridge should not drop rows, got {rows}"
        by_key = {r.business_key: r.val for r in rows}
        assert by_key == {"A": 100, "B": 200}, (
            f"Empty bridge should leave keys unchanged, got {by_key}"
        )

        spark.sql(f"DROP TABLE IF EXISTS {test_db}.bridge")

    def test_partial_mapping_resolves_some_preserves_rest(
        self, spark: SparkSession, test_db: str
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.bridge (
                business_key STRING, target_key STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.bridge VALUES ('A', 'C')
        """)

        bridge = IdentityBridgeConfig(
            table=f"{test_db}.bridge",
            join_on="business_key",
            target_column="target_key",
        )
        # A is mapped -> C; B is unmapped -> stays B.
        source = spark.createDataFrame(
            [("A", 100), ("B", 200)], "business_key string, val int"
        )

        resolved = SkeletonManager().apply_identity_bridge(_ctx(spark, bridge), source)
        by_key = {r.business_key: r.val for r in resolved.collect()}

        assert by_key.get("C") == 100, f"A should resolve to C, got {by_key}"
        assert by_key.get("B") == 200, (
            f"B should pass through unmapped, got {by_key}"
        )
        assert "A" not in by_key, f"A should not survive unresolved, got {by_key}"

        spark.sql(f"DROP TABLE IF EXISTS {test_db}.bridge")


class TestIdentityBridgeWiredInPipeline:
    """Guard the bridge wiring: a full run() must resolve the natural key."""

    def test_run_resolves_natural_key_through_bridge(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        # Source keyed by an ALTERNATE key (producer_id 100/200/300).
        spark.sql(f"""
            CREATE TABLE {test_db}.producers (
                producer_id INT, seller_city STRING, seller_state STRING,
                updated_at STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.producers VALUES
            (100, 'Sao Paulo', 'SP', '2024-01-01T00:00:00'),
            (200, 'Rio de Janeiro', 'RJ', '2024-01-01T00:00:00'),
            (300, 'Belo Horizonte', 'MG', '2024-01-01T00:00:00')
        """)

        # Bridge maps alternate -> canonical. 100/200/300 -> 1/2/3.
        spark.sql(f"""
            CREATE TABLE {test_db}.seller_bridge (
                producer_id INT, canonical_seller_id INT
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.seller_bridge VALUES
            (100, 1), (200, 2), (300, 3)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_seller
table_type: dimension
scd_type: 2
effective_at: updated_at
keys:
  surrogate_key: seller_sk
  natural_keys: [producer_id]
track_history_columns: [seller_city, seller_state]
identity_bridge:
  table: {test_db}.seller_bridge
  join_on: producer_id
  target_column: canonical_seller_id
sources:
  - name: {test_db}.producers
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT producer_id, seller_city, seller_state, updated_at FROM p
""")

        result = Orchestrator(config_path, spark=spark, etl_schema=test_db).run()
        assert result["status"] == "SUCCESS"

        # The dimension must be keyed by the RESOLVED canonical ids (1/2/3).
        # If the bridge were not wired into the pipeline, these would be the
        # unresolved producer ids (100/200/300).
        dim = (
            spark.table(f"{test_db}.dim_seller")
            .filter("producer_id > 0")
            .collect()
        )
        by_id = {r.producer_id: r for r in dim}
        assert set(by_id) == {1, 2, 3}, (
            f"Bridge did not resolve producer ids to canonical seller ids: {set(by_id)}"
        )
        assert by_id[1].seller_city == "Sao Paulo"
        assert by_id[2].seller_city == "Rio de Janeiro"
        assert by_id[3].seller_city == "Belo Horizonte"

        for t in [
            f"{test_db}.dim_seller",
            f"{test_db}.producers",
            f"{test_db}.seller_bridge",
        ]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")