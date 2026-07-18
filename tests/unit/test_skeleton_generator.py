from unittest.mock import MagicMock, patch

from kimball.processing.skeleton_generator import SkeletonGenerator


def test_skeleton_generator_table_not_exists():
    """Test that skeleton generation skips if dimension table doesn't exist."""
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False

    gen = SkeletonGenerator(spark)

    fact_df = MagicMock()
    gen.generate_skeletons(
        fact_df,
        "dim_customer",
        "customer_id",
        "customer_id",
        "customer_sk",
        batch_id="test-batch",
    )

    spark.catalog.tableExists.assert_called_once_with("dim_customer")


@patch("kimball.processing.key_generator.HashKeyGenerator.generate_keys")
def test_skeleton_generator_logic(mock_generate_keys):
    """Test skeleton generator with mocked PySpark functions."""
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True

    with (
        patch("kimball.processing.skeleton_generator.DeltaTable") as mock_dt,
        patch("kimball.processing.skeleton_generator.col"),
        patch("kimball.processing.skeleton_generator.lit"),
        patch("kimball.processing.skeleton_generator.current_timestamp"),
    ):
        mock_table = MagicMock()
        mock_dt.forName.return_value = mock_table

        fact_df = MagicMock()
        dim_df = MagicMock()
        mock_table.toDF.return_value = dim_df

        mock_field = MagicMock()
        mock_field.name = "other_col"
        mock_field.dataType = "string"
        mock_skeleton_field = MagicMock()
        mock_skeleton_field.name = "__is_skeleton"
        mock_skeleton_field.dataType = "boolean"
        dim_df.schema.fields = [mock_field, mock_skeleton_field]

        fact_keys = MagicMock()
        dim_keys = MagicMock()
        missing_keys = MagicMock()

        fact_df.select.return_value.distinct.return_value = fact_keys
        dim_df.select.return_value = dim_keys
        fact_keys.join.return_value = missing_keys

        mock_skeletons_out = MagicMock()
        mock_generate_keys.return_value = mock_skeletons_out

        missing_keys.isEmpty.return_value = True

        gen = SkeletonGenerator(spark)
        gen.generate_skeletons(
            fact_df,
            "dim_customer",
            "customer_id",
            "customer_id",
            "customer_sk",
            batch_id="test-batch",
        )

        missing_keys.withColumnRenamed.assert_not_called()

        missing_keys.isEmpty.return_value = False
        skeletons = MagicMock()
        missing_keys.withColumnRenamed.return_value = skeletons
        skeletons.withColumn.return_value = skeletons
        skeletons.drop.return_value = skeletons
        skeletons.select.return_value = skeletons

        gen.generate_skeletons(
            fact_df,
            "dim_customer",
            "customer_id",
            "customer_id",
            "customer_sk",
            batch_id="test-batch",
        )

        mock_table.alias.assert_called_with("target")
