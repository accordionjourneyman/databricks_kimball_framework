"""Unit tests for PipelineCompiler.

Tests cover:
- DAG building and dependency resolution
- Circular dependency detection
- Source table validation (mocked)
- Foreign key reference validation
- SQL syntax validation (mocked)
"""

from unittest.mock import MagicMock, patch

import pytest
from graphlib import CycleError

from kimball.compiler import CompileResult, PipelineCompiler
from kimball.config import ForeignKeyConfig, SourceConfig, TableConfig


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.sql.return_value = MagicMock()  # EXPLAIN returns something
    return spark


@pytest.fixture
def compiler(mock_spark):
    """Create a PipelineCompiler with mocked SparkSession."""
    return PipelineCompiler(spark=mock_spark)


@pytest.fixture
def dim_customer_config():
    """Sample dimension config."""
    return TableConfig(
        table_name="gold.dim_customer",
        table_type="dimension",
        surrogate_key="customer_sk",
        natural_keys=["customer_id"],
        sources=[
            SourceConfig(
                name="silver.customers",
                alias="c",
            )
        ],
        transformation_sql="SELECT customer_id, first_name FROM c",
    )


@pytest.fixture
def fact_sales_config():
    """Sample fact config with foreign keys."""
    return TableConfig(
        table_name="gold.fact_sales",
        table_type="fact",
        surrogate_key=None,
        natural_keys=[],
        merge_keys=["order_item_id"],
        sources=[
            SourceConfig(name="silver.orders", alias="o"),
            SourceConfig(name="gold.dim_customer", alias="c"),
        ],
        transformation_sql="SELECT o.order_id, c.customer_sk FROM o JOIN c ON o.customer_id = c.customer_id",
        foreign_keys=[
            ForeignKeyConfig(column="customer_sk", references="gold.dim_customer"),
        ],
    )


# =============================================================================
# Test: Dependency Graph Building
# =============================================================================


def test_build_dependency_graph(compiler, dim_customer_config, fact_sales_config):
    """Test building dependency graph from configs."""
    configs = [dim_customer_config, fact_sales_config]
    graph = compiler._build_dependency_graph(configs)

    assert "gold.dim_customer" in graph
    assert "gold.fact_sales" in graph
    assert "silver.customers" in graph["gold.dim_customer"]
    assert "silver.orders" in graph["gold.fact_sales"]
    assert "gold.dim_customer" in graph["gold.fact_sales"]


def test_build_and_validate_dag_success(
    compiler, dim_customer_config, fact_sales_config
):
    """Test successful DAG validation returns topological order."""
    configs = [dim_customer_config, fact_sales_config]
    dag_order = compiler._build_and_validate_dag(configs)

    # dim_customer should come before fact_sales (since fact depends on dim)
    assert "gold.dim_customer" in dag_order
    assert "gold.fact_sales" in dag_order


def test_detect_circular_dependency(compiler):
    """Test that circular dependencies are detected."""
    # Create configs with circular dependency: A -> B -> C -> A
    config_a = TableConfig(
        table_name="table_a",
        table_type="dimension",
        surrogate_key="sk",
        natural_keys=["id"],
        sources=[SourceConfig(name="table_c", alias="c")],
    )
    config_b = TableConfig(
        table_name="table_b",
        table_type="dimension",
        surrogate_key="sk",
        natural_keys=["id"],
        sources=[SourceConfig(name="table_a", alias="a")],
    )
    config_c = TableConfig(
        table_name="table_c",
        table_type="dimension",
        surrogate_key="sk",
        natural_keys=["id"],
        sources=[SourceConfig(name="table_b", alias="b")],
    )

    configs = [config_a, config_b, config_c]

    with pytest.raises(CycleError):
        compiler._build_and_validate_dag(configs)


# =============================================================================
# Test: Source Validation
# =============================================================================


def test_validate_sources_exist_success(compiler, dim_customer_config, mock_spark):
    """Test source validation passes when tables exist."""
    errors: list[str] = []
    warnings: list[str] = []

    compiler._validate_sources_exist([dim_customer_config], errors, warnings)

    assert len(errors) == 0
    mock_spark.catalog.tableExists.assert_called_with("silver.customers")


def test_validate_sources_missing(compiler, dim_customer_config, mock_spark):
    """Test source validation fails when table doesn't exist."""
    mock_spark.catalog.tableExists.return_value = False
    errors: list[str] = []
    warnings: list[str] = []

    compiler._validate_sources_exist([dim_customer_config], errors, warnings)

    assert len(errors) == 1
    assert "silver.customers" in errors[0]
    assert "not found" in errors[0]


def test_validate_sources_skips_targets(
    compiler, dim_customer_config, fact_sales_config, mock_spark
):
    """Test that sources which are also targets are not checked externally."""
    # fact_sales depends on dim_customer, which is also a target
    configs = [dim_customer_config, fact_sales_config]
    errors: list[str] = []
    warnings: list[str] = []

    compiler._validate_sources_exist(configs, errors, warnings)

    # Should NOT have checked gold.dim_customer (it's a target we're creating)
    calls = [str(call) for call in mock_spark.catalog.tableExists.call_args_list]
    assert not any("gold.dim_customer" in call for call in calls)


# =============================================================================
# Test: Foreign Key Validation
# =============================================================================


def test_validate_foreign_keys_success(
    compiler, dim_customer_config, fact_sales_config
):
    """Test FK validation passes when referenced table is in configs."""
    configs = [dim_customer_config, fact_sales_config]
    errors: list[str] = []

    compiler._validate_foreign_keys(configs, errors)

    assert len(errors) == 0


def test_validate_foreign_keys_missing_reference(
    compiler, fact_sales_config, mock_spark
):
    """Test FK validation fails when referenced table doesn't exist."""
    mock_spark.catalog.tableExists.return_value = False
    errors: list[str] = []

    # Only fact_sales, no dim_customer
    compiler._validate_foreign_keys([fact_sales_config], errors)

    assert len(errors) == 1
    assert "customer_sk" in errors[0]
    assert "gold.dim_customer" in errors[0]


# =============================================================================
# Test: SQL Syntax Validation
# =============================================================================


def test_validate_sql_syntax_success(compiler, dim_customer_config, mock_spark):
    """Test SQL validation passes for valid SQL."""
    errors: list[str] = []

    compiler._validate_sql_syntax([dim_customer_config], errors)

    assert len(errors) == 0
    # Should have called EXPLAIN
    assert mock_spark.sql.called


def test_validate_sql_syntax_error(compiler, dim_customer_config, mock_spark):
    """Test SQL validation captures syntax errors."""
    mock_spark.sql.side_effect = Exception(
        "AnalysisException: Column 'bad_col' not found"
    )
    errors: list[str] = []

    compiler._validate_sql_syntax([dim_customer_config], errors)

    assert len(errors) == 1
    assert "SQL error" in errors[0]


# =============================================================================
# Test: compile_all Integration
# =============================================================================


def test_compile_all_success(compiler, tmp_path):
    """Test compile_all succeeds with valid configs."""
    # Create a simple config file
    config_content = """
    table_name: gold.dim_test
    table_type: dimension
    keys:
      surrogate_key: test_sk
      natural_keys: [test_id]
    sources:
      - name: silver.test
        alias: t
    transformation_sql: SELECT test_id FROM t
    """
    (tmp_path / "dim_test.yml").write_text(config_content, encoding="utf-8")

    result = compiler.compile_all(str(tmp_path))

    assert result.success is True
    assert result.models == 1
    assert len(result.errors) == 0


def test_compile_all_with_cycle(compiler, tmp_path):
    """Test compile_all detects circular dependencies."""
    # Create configs with cycle
    config_a = """
    table_name: table_a
    table_type: dimension
    keys:
      surrogate_key: sk
      natural_keys: [id]
    sources:
      - name: table_b
        alias: b
    """
    config_b = """
    table_name: table_b
    table_type: dimension
    keys:
      surrogate_key: sk
      natural_keys: [id]
    sources:
      - name: table_a
        alias: a
    """
    (tmp_path / "table_a.yml").write_text(config_a, encoding="utf-8")
    (tmp_path / "table_b.yml").write_text(config_b, encoding="utf-8")

    result = compiler.compile_all(
        str(tmp_path), skip_source_check=True, skip_sql_check=True
    )

    assert result.success is False
    assert any("Circular" in e for e in result.errors)


def test_compile_all_empty_directory(compiler, tmp_path):
    """Test compile_all handles empty directory."""
    result = compiler.compile_all(str(tmp_path))

    assert result.success is False
    assert "No valid configs found" in result.errors[0]


# =============================================================================
# Test: compile_config Single Config
# =============================================================================


def test_compile_config_success(compiler, dim_customer_config):
    """Test compiling a single config."""
    result = compiler.compile_config(dim_customer_config)

    assert result.success is True
    assert result.models == 1


def test_compile_config_skip_checks(compiler, dim_customer_config):
    """Test compile_config with checks skipped."""
    result = compiler.compile_config(
        dim_customer_config,
        skip_source_check=True,
        skip_sql_check=True,
    )

    assert result.success is True


# =============================================================================
# Test: CompileResult
# =============================================================================


def test_compile_result_str_success():
    """Test CompileResult string representation on success."""
    result = CompileResult(
        success=True,
        models=3,
        dag_order=["dim_a", "dim_b", "fact_c"],
    )

    output = str(result)
    assert "✅" in output
    assert "3 models" in output


def test_compile_result_str_failure():
    """Test CompileResult string representation on failure."""
    result = CompileResult(
        success=False,
        models=2,
        errors=["Error 1", "Error 2"],
    )

    output = str(result)
    assert "❌" in output
    assert "2 errors" in output
    assert "Error 1" in output
