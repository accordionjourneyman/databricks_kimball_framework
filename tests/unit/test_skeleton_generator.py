from unittest.mock import MagicMock, patch

def test_skeleton_generator_table_not_exists():
    """Test that skeleton generation skips if dimension table doesn't exist."""
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    
    from kimball.skeleton_generator import SkeletonGenerator
    gen = SkeletonGenerator(spark)
    
    fact_df = MagicMock()
    gen.generate_skeletons(fact_df, "dim_customer", "customer_id", "customer_id", "customer_sk", "identity")
    
    # Should not attempt to access DeltaTable
    spark.catalog.tableExists.assert_called_once_with("dim_customer")

def test_skeleton_generator_logic():
    """Test skeleton generator with mocked PySpark functions."""
    with patch("kimball.skeleton_generator.DeltaTable") as mock_dt, \
         patch("kimball.skeleton_generator.col") as mock_col, \
         patch("kimball.skeleton_generator.lit") as mock_lit, \
         patch("kimball.skeleton_generator.current_timestamp") as mock_ts, \
         patch("kimball.skeleton_generator.to_date") as mock_date:
        
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        
        # Mock DeltaTable
        mock_table = MagicMock()
        mock_dt.forName.return_value = mock_table
        
        # Mock DataFrames
        fact_df = MagicMock()
        dim_df = MagicMock()
        mock_table.toDF.return_value = dim_df
        
        # Mock schema
        mock_field = MagicMock()
        mock_field.name = "other_col"
        mock_field.dataType = "string"
        dim_df.schema.fields = [mock_field]
        
        # Mock key operations
        fact_keys = MagicMock()
        dim_keys = MagicMock()
        missing_keys = MagicMock()
        
        fact_df.select.return_value.distinct.return_value = fact_keys
        dim_df.select.return_value.distinct.return_value = dim_keys
        fact_keys.join.return_value = missing_keys
        
        # Test case 1: No missing keys
        missing_keys.isEmpty.return_value = True
        
        from kimball.skeleton_generator import SkeletonGenerator
        gen = SkeletonGenerator(spark)
        gen.generate_skeletons(fact_df, "dim_customer", "customer_id", "customer_id", "customer_sk", "identity")
        
        # Should not attempt to write
        missing_keys.withColumnRenamed.assert_not_called()
        
        # Test case 2: With missing keys
        missing_keys.isEmpty.return_value = False
        skeletons = MagicMock()
        missing_keys.withColumnRenamed.return_value = skeletons
        skeletons.withColumn.return_value = skeletons
        skeletons.drop.return_value = skeletons
        skeletons.select.return_value = skeletons
        
        gen.generate_skeletons(fact_df, "dim_customer", "customer_id", "customer_id", "customer_sk", "identity")
        
        # Verify write was called
        skeletons.write.format.assert_called_with("delta")
