from typing import Literal
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    TimestampType,
)

from kimball.common.config import (
    ForeignKeyConfig,
    ForeignKeyLookupConfig,
    NullPolicyConfig,
)
from kimball.common.errors import DataQualityError
from kimball.processing.key_broker import KeyBroker, _any_null, _placeholder


def test_key_broker_reads_dimension_at_pinned_repair_version() -> None:
    spark = MagicMock()
    reader = spark.read.format.return_value
    reader.option.return_value = reader
    frame = reader.option.return_value.table.return_value
    broker = KeyBroker(spark, snapshot_versions={"gold.dim_customer": 42})

    assert broker._read_table("gold.dim_customer") is frame
    spark.read.format.assert_called_once_with("delta")
    reader.option.assert_called_once_with("versionAsOf", 42)
    reader.table.assert_called_once_with("gold.dim_customer")


@pytest.mark.parametrize(
    "data_type",
    [
        StringType(),
        IntegerType(),
        DecimalType(10, 2),
        DoubleType(),
        BooleanType(),
        TimestampType(),
        DateType(),
    ],
)
def test_skeleton_placeholder_is_concrete_for_supported_types(data_type) -> None:
    literal = MagicMock()
    with patch("kimball.processing.key_broker.F.lit", return_value=literal):
        result = _placeholder(StructField("attribute", data_type))

    literal.cast.assert_called_once_with(data_type)
    assert result == literal.cast.return_value


def test_skeleton_placeholder_rejects_complex_type_without_substitute() -> None:
    with pytest.raises(ValueError, match="explicit substitute"):
        _placeholder(StructField("tags", ArrayType(StringType())))


def test_any_null_builds_one_combined_expression() -> None:
    with patch("kimball.processing.key_broker.F") as functions:
        functions.col.return_value.isNull.return_value = MagicMock()
        result = _any_null(["supplier_id", "tenant_id"])

    assert functions.col.call_count == 2
    assert result is not None


def _standard_fk(
    *, early_arriving: Literal["skeleton", "default", "error"] = "default"
) -> ForeignKeyConfig:
    return ForeignKeyConfig(
        column="customer_sk",
        references="gold.dim_customer",
        dimension_key="customer_sk",
        lookup=ForeignKeyLookupConfig(
            source_columns=["supplier_customer_id"],
            early_arriving=early_arriving,
        ),
    )


def test_broker_orchestrates_lookup_without_generating_fact_keys() -> None:
    fact = MagicMock()
    fact.columns = ["supplier_customer_id"]
    mapped = MagicMock()
    resolved = MagicMock()
    broker = KeyBroker(MagicMock())
    with (
        patch.object(
            KeyBroker, "_apply_identity_map", return_value=(mapped, ["original_id"])
        ),
        patch.object(
            KeyBroker, "_resolve_relationship", return_value=resolved
        ) as mock_resolve,
    ):
        result = broker.resolve_fact_keys(
            fact,
            [_standard_fk()],
            batch_id="batch-1",
            null_policy=NullPolicyConfig(),
            fact_table="gold.fact_sales",
            fact_grain=["order_id"],
            source_version=4,
        )

    assert result is resolved
    mock_resolve.assert_called_once()
    kwargs = mock_resolve.call_args.kwargs
    assert kwargs["source_version"] == 4
    assert kwargs["original_identity_columns"] == ["original_id"]


def test_broker_runs_inferred_member_step_before_type7_lookup() -> None:
    fact = MagicMock()
    fact.columns = ["customer_id", "order_at"]
    fk = ForeignKeyConfig(
        column="customer_sk",
        references="gold.dim_customer",
        dimension_key="customer_sk",
        relationship="type7",
        durable_column="customer_dk",
        durable_dimension_key="customer_dk",
        lookup=ForeignKeyLookupConfig(
            source_columns=["customer_id"],
            event_time="order_at",
            early_arriving="skeleton",
        ),
    )
    broker = KeyBroker(MagicMock())
    with (
        patch.object(KeyBroker, "_apply_identity_map", return_value=(fact, [])),
        patch.object(KeyBroker, "_ensure_skeletons") as mock_skeletons,
        patch.object(
            KeyBroker, "_resolve_relationship", return_value=fact
        ) as mock_resolve,
    ):
        broker.resolve_fact_keys(
            fact,
            [fk],
            batch_id="batch-1",
            null_policy=NullPolicyConfig(),
        )

        mock_skeletons.assert_called_once()
        mock_resolve.assert_called_once()


def test_broker_fails_closed_on_missing_supplier_columns() -> None:
    fact = MagicMock()
    fact.columns = ["order_id"]

    with pytest.raises(DataQualityError, match="supplier_customer_id"):
        KeyBroker(MagicMock()).resolve_fact_keys(
            fact,
            [_standard_fk()],
            batch_id="batch-1",
            null_policy=NullPolicyConfig(),
        )


def test_broker_skips_metadata_only_relationships() -> None:
    fact = MagicMock()
    broker = KeyBroker(MagicMock())
    with patch.object(KeyBroker, "_resolve_relationship") as mock_resolve:
        assert (
            broker.resolve_fact_keys(
                fact,
                [ForeignKeyConfig(column="customer_sk")],
                batch_id="batch-1",
                null_policy=NullPolicyConfig(),
            )
            is fact
        )
        mock_resolve.assert_not_called()


def test_table_version_is_best_effort() -> None:
    spark = MagicMock()
    spark.sql.return_value.select.return_value.first.return_value = {"version": 7}
    broker = KeyBroker(spark)

    assert broker._table_version("gold.dim_customer") == 7

    spark.sql.side_effect = RuntimeError("history unavailable")
    assert broker._table_version("gold.dim_customer") == -1


# ------------------------------------------------------------------ #
# _validate_resolution tests                                          #
# ------------------------------------------------------------------ #


def _make_fk(*, detect_fanout: bool = True, validate_resolution: bool = False):
    return ForeignKeyConfig(
        column="customer_sk",
        references="gold.dim_customer",
        dimension_key="customer_sk",
        lookup=ForeignKeyLookupConfig(
            source_columns=["customer_id"],
            early_arriving="default",
            detect_fanout=detect_fanout,
            validate_resolution=validate_resolution,
        ),
    )


def _make_joined_df(total: int, resolved: int):
    """Create a mock joined DataFrame whose agg().collect() returns known stats."""
    joined = MagicMock()
    row = MagicMock()
    row.__getitem__ = lambda self, key: {"total": total, "resolved": resolved}[key]
    joined.agg.return_value.collect.return_value = [row]
    return joined


def _make_fact_df(nk_count: int = 0):
    """Create a mock fact DataFrame for count assertions."""
    fact = MagicMock()
    fact.columns = ["customer_id"]
    if nk_count > 0:
        fact.select.return_value.distinct.return_value.count.return_value = nk_count
    return fact


def _make_dim_spark(duplicate_keys: bool = False):
    """Create a mock Spark session whose table() returns dimension data."""
    spark = MagicMock()
    dim = MagicMock()
    if duplicate_keys:
        row = MagicMock()
        row.__getitem__ = lambda self, key: {"customer_id": 1}[key]
        dim.groupBy.return_value.agg.return_value.filter.return_value.limit.return_value.collect.return_value = [
            row
        ]
    else:
        dim.groupBy.return_value.agg.return_value.filter.return_value.limit.return_value.collect.return_value = []
    spark.table.return_value = dim
    return spark


def test_resolution_rate_is_logged(caplog) -> None:
    spark = _make_dim_spark()
    broker = KeyBroker(spark)
    joined = _make_joined_df(total=100, resolved=95)

    with caplog.at_level("INFO", logger="kimball.processing.key_broker"):
        broker._validate_resolution(_make_fact_df(), joined, _make_fk())

    assert "95/100 resolved (95.0%)" in caplog.text
    assert "5 sentinels" in caplog.text


def test_resolution_stats_are_skipped_when_info_logging_is_disabled(caplog) -> None:
    spark = _make_dim_spark()
    broker = KeyBroker(spark)
    joined = _make_joined_df(total=100, resolved=95)

    with caplog.at_level("WARNING", logger="kimball.processing.key_broker"):
        broker._validate_resolution(
            _make_fact_df(), joined, _make_fk(detect_fanout=False)
        )

    joined.agg.assert_not_called()
    assert "Key resolution for" not in caplog.text


def test_fanout_raises_on_duplicate_dimension_keys() -> None:
    spark = _make_dim_spark(duplicate_keys=True)
    broker = KeyBroker(spark)
    joined = _make_joined_df(total=100, resolved=100)

    with pytest.raises(DataQualityError, match="Fanout detected"):
        broker._validate_resolution(_make_fact_df(), joined, _make_fk())


def test_fanout_check_skipped_when_disabled() -> None:
    spark = _make_dim_spark(duplicate_keys=True)
    broker = KeyBroker(spark)
    joined = _make_joined_df(total=100, resolved=100)

    broker._validate_resolution(_make_fact_df(), joined, _make_fk(detect_fanout=False))


def test_validate_resolution_raises_on_unresolved_nks() -> None:
    spark = _make_dim_spark()
    broker = KeyBroker(spark)
    joined = _make_joined_df(total=100, resolved=80)
    stats = MagicMock()
    stats.__getitem__ = lambda self, key: {
        "fact_nk_distinct": 10,
        "resolved_nk_distinct": 8,
    }[key]
    joined.groupBy.return_value.agg.return_value.agg.return_value.first.return_value = (
        stats
    )

    with pytest.raises(DataQualityError, match="Resolution count mismatch"):
        broker._validate_resolution(
            _make_fact_df(), joined, _make_fk(validate_resolution=True)
        )


def test_validate_resolution_passes_when_all_nks_resolve() -> None:
    spark = _make_dim_spark()
    broker = KeyBroker(spark)
    joined = _make_joined_df(total=100, resolved=100)
    stats = MagicMock()
    stats.__getitem__ = lambda self, key: {
        "fact_nk_distinct": 10,
        "resolved_nk_distinct": 10,
    }[key]
    joined.groupBy.return_value.agg.return_value.agg.return_value.first.return_value = (
        stats
    )

    broker._validate_resolution(
        _make_fact_df(), joined, _make_fk(validate_resolution=True)
    )


def test_validate_resolution_skipped_by_default() -> None:
    spark = _make_dim_spark()
    broker = KeyBroker(spark)
    joined = _make_joined_df(total=100, resolved=80)
    fact = _make_fact_df(nk_count=10)

    broker._validate_resolution(fact, joined, _make_fk())


@pytest.mark.spark
def test_resolution_validation_groups_once_across_join_fanout(spark) -> None:
    fk = _make_fk(detect_fanout=False, validate_resolution=True)
    fact = spark.createDataFrame([("A",), ("A",), ("B",)], "customer_id string")
    joined = spark.createDataFrame(
        [("A", 10), ("A", 11), ("B", 20)], "customer_id string, customer_sk long"
    )

    KeyBroker(spark)._validate_resolution(fact, joined, fk)


@pytest.mark.spark
def test_resolution_validation_reports_distinct_unresolved_keys(spark) -> None:
    fk = _make_fk(detect_fanout=False, validate_resolution=True)
    fact = spark.createDataFrame(
        [("A",), ("A",), ("B",), (None,)], "customer_id string"
    )
    joined = spark.createDataFrame(
        [("A", 10), ("A", -1), ("B", -1), (None, -1)],
        "customer_id string, customer_sk long",
    )

    with pytest.raises(
        DataQualityError,
        match=r"3 distinct NKs in fact, 1 resolved\. 2 unresolved\.",
    ):
        KeyBroker(spark)._validate_resolution(fact, joined, fk)


@pytest.mark.spark
def test_deferred_resolution_metrics_are_logged_after_delta_merge(
    spark, tmp_path, caplog
) -> None:
    from delta.tables import DeltaTable

    target_path = str(tmp_path / "observation_metrics")
    spark.createDataFrame([], "customer_id string, customer_sk long").write.format(
        "delta"
    ).save(target_path)
    fact = spark.createDataFrame([("A",), ("B",)], "customer_id string")
    joined = spark.createDataFrame(
        [("A", 10), ("B", -1)], "customer_id string, customer_sk long"
    )
    broker = KeyBroker(spark, defer_resolution_metrics=True)

    with caplog.at_level("INFO", logger="kimball.processing.key_broker"):
        resolved = broker._validate_resolution(
            fact, joined, _make_fk(detect_fanout=False), index=0
        )
        # Model downstream validation reads before observations are attached.
        resolved.limit(1).collect()
        observed = broker.observe_resolution_metrics(resolved)
        DeltaTable.forPath(spark, target_path).alias("target").merge(
            observed.alias("source"), "target.customer_id = source.customer_id"
        ).whenNotMatchedInsertAll().execute()
        broker.log_resolution_metrics()

    assert (
        "Key resolution for customer_sk: 1/2 resolved (50.0%), 1 sentinels"
        in caplog.text
    )
    assert spark.read.format("delta").load(target_path).count() == 2
