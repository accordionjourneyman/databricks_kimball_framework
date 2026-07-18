from unittest.mock import MagicMock

from kimball.observability.unresolved_keys import UnresolvedKeyRegistry


def test_registry_ddl_is_non_null_and_captures_replay_evidence() -> None:
    spark = MagicMock()
    registry = UnresolvedKeyRegistry(spark, "etl")

    registry.ensure_table()

    ddl = spark.sql.call_args.args[0]
    assert "fact_table STRING NOT NULL" in ddl
    assert "fact_grain_hash STRING NOT NULL" in ddl
    assert "source_identity_json STRING NOT NULL" in ddl
    assert "event_time TIMESTAMP NOT NULL" in ddl
    assert "status STRING NOT NULL" in ddl


def test_registry_deduplicates_fact_grain_before_delta_merge() -> None:
    source = MagicMock()
    selected = MagicMock()
    deduplicated = MagicMock()
    source.select.return_value = selected
    selected.dropDuplicates.return_value = deduplicated
    spark = MagicMock()
    registry = UnresolvedKeyRegistry(spark, "etl")

    # Delta execution is outside this unit''s scope; the call proves the source
    # is normalized to one row per target MERGE key first.
    from unittest.mock import patch

    with patch("kimball.observability.unresolved_keys.DeltaTable") as delta:
        delta.forName.return_value.alias.return_value.merge.return_value.whenMatchedUpdate.return_value.whenNotMatchedInsertAll.return_value.execute.return_value = None
        registry.record(
            source,
            fact_table="gold.fact_sales",
            relationship="customer_sk",
            fact_grain=["order_id"],
            source_identity=["customer_id"],
            event_time=None,
            batch_id="batch-1",
            source_version=1,
            dimension_version=2,
        )

    selected.dropDuplicates.assert_called_once_with(
        ["fact_table", "relationship", "fact_grain_hash"]
    )
