import pytest

from kimball.common.config import ConfigLoader


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
    assert "surrogate_key" in str(excinfo.value)
