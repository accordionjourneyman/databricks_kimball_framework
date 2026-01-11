import pytest

from kimball.config import ConfigLoader


def test_load_config_valid_fact(tmp_path):
    # Kimball: Facts do NOT have surrogate keys - they use merge_keys (degenerate dimensions)
    config_content = """
    table_name: {{ env }}_gold.fact_sales
    table_type: fact
    merge_keys: [transaction_id]
    sources:
      - name: silver.transactions
        alias: t
    """
    config_file = tmp_path / "test_config.yml"
    config_file.write_text(config_content, encoding="utf-8")

    # Load with env vars
    loader = ConfigLoader(env_vars={"env": "prod"})
    config = loader.load_config(str(config_file))

    assert config.table_name == "prod_gold.fact_sales"
    assert config.table_type == "fact"
    assert config.surrogate_key is None  # Facts have no SK
    assert config.merge_keys == ["transaction_id"]
    assert len(config.sources) == 1
    assert config.sources[0].name == "silver.transactions"


def test_load_config_valid_dimension(tmp_path):
    # Kimball: Dimensions MUST have surrogate_key and natural_keys
    config_content = """
    table_name: {{ env }}_gold.dim_customer
    table_type: dimension
    keys:
      surrogate_key: customer_sk
      natural_keys: [customer_id]
    sources:
      - name: silver.customers
        alias: c
    """
    config_file = tmp_path / "test_config.yml"
    config_file.write_text(config_content, encoding="utf-8")

    loader = ConfigLoader(env_vars={"env": "prod"})
    config = loader.load_config(str(config_file))

    assert config.table_name == "prod_gold.dim_customer"
    assert config.table_type == "dimension"
    assert config.surrogate_key == "customer_sk"
    assert config.natural_keys == ["customer_id"]


def test_load_config_dimension_missing_keys(tmp_path):
    # Kimball: Dimensions without keys should fail validation
    config_content = """
    table_name: dim_customer
    table_type: dimension
    sources:
      - name: silver.customers
    """
    config_file = tmp_path / "bad_config.yml"
    config_file.write_text(config_content, encoding="utf-8")

    loader = ConfigLoader()
    with pytest.raises(ValueError) as excinfo:
        loader.load_config(str(config_file))
    # JSON Schema validation fails on missing 'keys' for dimensions
    assert "keys" in str(excinfo.value) or "surrogate_key" in str(excinfo.value)


def test_load_config_with_tests_block(tmp_path):
    """Test parsing of dbt-style tests block."""
    config_content = """
    table_name: gold.dim_customer
    table_type: dimension
    keys:
      surrogate_key: customer_sk
      natural_keys: [customer_id]
    sources:
      - name: silver.customers
        alias: c
    tests:
      - column: customer_sk
        tests:
          - unique
          - not_null
      - column: status
        tests:
          - accepted_values: [active, inactive, pending]
    """
    config_file = tmp_path / "test_config.yml"
    config_file.write_text(config_content, encoding="utf-8")

    loader = ConfigLoader()
    config = loader.load_config(str(config_file))

    assert config.tests is not None
    assert len(config.tests) == 2

    # First test block
    assert config.tests[0].column == "customer_sk"
    assert config.tests[0].tests == ["unique", "not_null"]

    # Second test block with dict test
    assert config.tests[1].column == "status"
    assert len(config.tests[1].tests) == 1
    assert config.tests[1].tests[0] == {
        "accepted_values": ["active", "inactive", "pending"]
    }


def test_load_config_with_relationships_test(tmp_path):
    """Test parsing relationships test in tests block."""
    config_content = """
    table_name: gold.fact_sales
    table_type: fact
    merge_keys: [order_id]
    sources:
      - name: silver.orders
        alias: o
    tests:
      - column: customer_sk
        tests:
          - relationships:
              to: gold.dim_customer
              field: customer_sk
    """
    config_file = tmp_path / "test_config.yml"
    config_file.write_text(config_content, encoding="utf-8")

    loader = ConfigLoader()
    config = loader.load_config(str(config_file))

    assert config.tests is not None
    assert len(config.tests) == 1
    assert config.tests[0].column == "customer_sk"
    assert config.tests[0].tests[0] == {
        "relationships": {"to": "gold.dim_customer", "field": "customer_sk"}
    }


def test_load_config_without_tests(tmp_path):
    """Test that configs without tests block work correctly."""
    config_content = """
    table_name: gold.dim_product
    table_type: dimension
    keys:
      surrogate_key: product_sk
      natural_keys: [product_id]
    sources:
      - name: silver.products
        alias: p
    """
    config_file = tmp_path / "test_config.yml"
    config_file.write_text(config_content, encoding="utf-8")

    loader = ConfigLoader()
    config = loader.load_config(str(config_file))

    assert config.tests is None
