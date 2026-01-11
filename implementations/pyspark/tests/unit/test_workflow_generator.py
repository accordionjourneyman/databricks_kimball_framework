"""
Unit tests for WorkflowGenerator.

Tests config scanning, dependency graph building, and DAB YAML generation.
"""

import os
import tempfile

import pytest

from kimball.workflow_generator import TaskDefinition, WorkflowGenerator


@pytest.fixture
def temp_config_dir():
    """Create temporary directory with test configs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create dimension config
        dim_config = """
table_name: gold.dim_customer
table_type: dimension
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
sources:
  - name: silver.customers
"""
        with open(os.path.join(tmpdir, "dim_customer.yml"), "w") as f:
            f.write(dim_config)

        # Create fact config
        fact_config = """
table_name: gold.fact_sales
table_type: fact
merge_keys: [order_item_id]
sources:
  - name: silver.order_items
"""
        with open(os.path.join(tmpdir, "fact_sales.yml"), "w") as f:
            f.write(fact_config)

        yield tmpdir


@pytest.fixture
def generator():
    """Create WorkflowGenerator instance."""
    return WorkflowGenerator()


class TestTaskDefinition:
    """Tests for TaskDefinition dataclass."""

    def test_task_definition_creation(self):
        """Should create task definition with all fields."""
        task = TaskDefinition(
            task_key="gold_dim_customer",
            table_name="gold.dim_customer",
            table_type="dimension",
            config_path="/path/to/config.yml",
            dependencies=["gold_dim_product"],
        )
        assert task.task_key == "gold_dim_customer"
        assert task.table_type == "dimension"

    def test_task_definition_defaults(self):
        """Dependencies should default to empty list."""
        task = TaskDefinition(
            task_key="test",
            table_name="test",
            table_type="dimension",
            config_path="/path",
        )
        assert task.dependencies == []


class TestScanConfigs:
    """Tests for scan_configs method."""

    def test_scan_valid_configs(self, generator, temp_config_dir):
        """Should scan and parse valid configs."""
        tasks, errors = generator.scan_configs(temp_config_dir)

        assert len(tasks) == 2
        assert len(errors) == 0

        task_names = [t.table_name for t in tasks]
        assert "gold.dim_customer" in task_names
        assert "gold.fact_sales" in task_names

    def test_scan_empty_directory(self, generator):
        """Should return empty lists for empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tasks, errors = generator.scan_configs(tmpdir)
            assert tasks == []
            assert errors == []

    def test_scan_with_invalid_config(self, generator):
        """Should collect errors for invalid configs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Invalid config - missing required fields
            invalid_config = """
table_name: gold.broken
table_type: dimension
# Missing keys
sources:
  - name: silver.test
"""
            with open(os.path.join(tmpdir, "broken.yml"), "w") as f:
                f.write(invalid_config)

            tasks, errors = generator.scan_configs(tmpdir)

            assert len(tasks) == 0
            assert len(errors) == 1
            assert "broken.yml" in errors[0][0]

    def test_scan_fail_fast(self, generator):
        """Should raise immediately with fail_fast=True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            invalid_config = """
table_name: gold.broken
table_type: invalid_type
"""
            with open(os.path.join(tmpdir, "broken.yml"), "w") as f:
                f.write(invalid_config)

            with pytest.raises(ValueError) as exc_info:
                generator.scan_configs(tmpdir, fail_fast=True)

            assert "Failed to parse config" in str(exc_info.value)

    def test_scan_mixed_valid_invalid(self, generator, temp_config_dir):
        """Should collect valid configs even when some fail."""
        # Add an invalid config to the existing valid ones
        invalid_config = """
table_name: gold.broken
table_type: invalid_type
"""
        with open(os.path.join(temp_config_dir, "broken.yml"), "w") as f:
            f.write(invalid_config)

        tasks, errors = generator.scan_configs(temp_config_dir)

        assert len(tasks) == 2  # The two valid configs
        assert len(errors) == 1  # The one broken config


class TestBuildDependencyGraph:
    """Tests for build_dependency_graph method."""

    def test_facts_depend_on_dimensions(self, generator):
        """Facts should depend on all dimensions."""
        tasks = [
            TaskDefinition("dim_a", "gold.dim_a", "dimension", "/path/a"),
            TaskDefinition("dim_b", "gold.dim_b", "dimension", "/path/b"),
            TaskDefinition("fact_x", "gold.fact_x", "fact", "/path/x"),
        ]

        result = generator.build_dependency_graph(tasks)

        # Find the fact task
        fact_task = next(t for t in result if t.table_type == "fact")
        assert set(fact_task.dependencies) == {"dim_a", "dim_b"}

    def test_dimensions_have_no_dependencies(self, generator):
        """Dimensions should have no dependencies."""
        tasks = [
            TaskDefinition("dim_a", "gold.dim_a", "dimension", "/path/a"),
            TaskDefinition("fact_x", "gold.fact_x", "fact", "/path/x"),
        ]

        result = generator.build_dependency_graph(tasks)

        dim_task = next(t for t in result if t.table_type == "dimension")
        assert dim_task.dependencies == []


class TestSanitizeTaskKey:
    """Tests for _sanitize_task_key method."""

    def test_dots_replaced(self, generator):
        """Dots should be replaced with underscores."""
        assert generator._sanitize_task_key("gold.dim_customer") == "gold_dim_customer"

    def test_dashes_replaced(self, generator):
        """Dashes should be replaced with underscores."""
        assert generator._sanitize_task_key("my-table") == "my_table"

    def test_combined(self, generator):
        """Both dots and dashes should be replaced."""
        assert (
            generator._sanitize_task_key("my-catalog.db.table") == "my_catalog_db_table"
        )


class TestGenerateDabYaml:
    """Tests for generate_dab_yaml method."""

    def test_generates_valid_structure(self, generator, temp_config_dir):
        """Should generate valid DAB structure."""
        dab = generator.generate_dab_yaml(temp_config_dir)

        assert "resources" in dab
        assert "jobs" in dab["resources"]
        assert "nightly_etl" in dab["resources"]["jobs"]

        job = dab["resources"]["jobs"]["nightly_etl"]
        assert "tasks" in job
        assert "job_clusters" in job

    def test_dimension_task_present(self, generator, temp_config_dir):
        """Should include load_dimensions task."""
        dab = generator.generate_dab_yaml(temp_config_dir)

        tasks = dab["resources"]["jobs"]["nightly_etl"]["tasks"]
        task_keys = [t["task_key"] for t in tasks]
        assert "load_dimensions" in task_keys

    def test_fact_depends_on_dimensions(self, generator, temp_config_dir):
        """load_facts should depend on load_dimensions."""
        dab = generator.generate_dab_yaml(temp_config_dir)

        tasks = dab["resources"]["jobs"]["nightly_etl"]["tasks"]
        fact_task = next(t for t in tasks if t["task_key"] == "load_facts")

        assert "depends_on" in fact_task
        assert {"task_key": "load_dimensions"} in fact_task["depends_on"]

    def test_fair_scheduler_configured(self, generator, temp_config_dir):
        """FAIR scheduler should be configured in cluster."""
        dab = generator.generate_dab_yaml(temp_config_dir)

        cluster = dab["resources"]["jobs"]["nightly_etl"]["job_clusters"][0]
        spark_conf = cluster["new_cluster"]["spark_conf"]
        assert spark_conf.get("spark.scheduler.mode") == "FAIR"

    def test_custom_job_name(self, generator, temp_config_dir):
        """Should use custom job name when provided."""
        dab = generator.generate_dab_yaml(temp_config_dir, job_name="My Custom ETL")

        job_name = dab["resources"]["jobs"]["nightly_etl"]["name"]
        assert job_name == "My Custom ETL"

    def test_raises_on_all_configs_invalid(self, generator):
        """Should raise when no valid configs found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Only invalid config
            with open(os.path.join(tmpdir, "broken.yml"), "w") as f:
                f.write("table_name: broken\ntable_type: invalid")

            with pytest.raises(ValueError) as exc_info:
                generator.generate_dab_yaml(tmpdir)

            assert "No valid configs" in str(exc_info.value)


class TestGenerateIndividualTasks:
    """Tests for generate_individual_tasks method."""

    def test_one_task_per_table(self, generator, temp_config_dir):
        """Should generate one task per table."""
        tasks = generator.generate_individual_tasks(temp_config_dir)

        assert len(tasks) == 2
        task_keys = [t["task_key"] for t in tasks]
        assert "gold_dim_customer" in task_keys
        assert "gold_fact_sales" in task_keys

    def test_task_has_notebook_path(self, generator, temp_config_dir):
        """Each task should have notebook_task with path."""
        tasks = generator.generate_individual_tasks(temp_config_dir)

        for task in tasks:
            assert "notebook_task" in task
            assert "notebook_path" in task["notebook_task"]


class TestSaveDabYaml:
    """Tests for save_dab_yaml method."""

    def test_writes_yaml_file(self, generator, temp_config_dir):
        """Should write valid YAML file."""
        import yaml

        output_path = os.path.join(temp_config_dir, "databricks.yml")
        generator.save_dab_yaml(temp_config_dir, output_path)

        assert os.path.exists(output_path)

        with open(output_path) as f:
            content = yaml.safe_load(f)

        assert "resources" in content
