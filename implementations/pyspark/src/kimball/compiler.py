"""
Pipeline Compiler - Pre-run validation for Kimball Framework.

Provides dbt-like "compile before run" semantics:
- DAG resolution with cycle detection
- Source table existence validation
- SQL syntax validation (via EXPLAIN)
- Foreign key reference validation

Usage:
    from kimball.compiler import PipelineCompiler

    compiler = PipelineCompiler()
    result = compiler.compile_all("/path/to/configs/")
    if result.success:
        print(f"Compiled {result.models} models")
    else:
        print(f"Errors: {result.errors}")
"""

from __future__ import annotations

import glob
import os
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from typing import TYPE_CHECKING

from kimball.config import ConfigLoader, TableConfig
from kimball.errors import ConfigurationError, NonRetriableError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class CompileError(NonRetriableError):
    """Compilation failed - errors found during pre-run validation."""

    pass


@dataclass
class CompileResult:
    """Result of pipeline compilation."""

    success: bool
    models: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    dag_order: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        if self.success:
            return (
                f"✅ Compiled {self.models} models. Execution order: {self.dag_order}"
            )
        return f"❌ Compilation failed with {len(self.errors)} errors:\n" + "\n".join(
            f"  - {e}" for e in self.errors
        )


class PipelineCompiler:
    """Pre-run validation without executing any SQL.

    Performs compile-time checks similar to dbt:
    - DAG resolution with circular dependency detection
    - Source table existence validation
    - SQL syntax validation via EXPLAIN
    - Foreign key reference validation
    """

    def __init__(self, spark: SparkSession | None = None):
        """Initialize compiler.

        Args:
            spark: SparkSession for catalog/SQL validation.
                   If None, will attempt to get from databricks.sdk.runtime.
        """
        self._spark = spark
        self._loader = ConfigLoader()

    @property
    def spark(self) -> SparkSession:
        """Lazy-load SparkSession."""
        if self._spark is None:
            from databricks.sdk.runtime import spark

            self._spark = spark
        return self._spark

    def compile_all(
        self,
        config_dir: str,
        skip_source_check: bool = False,
        skip_sql_check: bool = False,
    ) -> CompileResult:
        """Validate all configs in a directory.

        Args:
            config_dir: Path to directory containing YAML config files.
            skip_source_check: Skip source table existence check (for offline validation).
            skip_sql_check: Skip SQL syntax validation (for offline validation).

        Returns:
            CompileResult with success status, errors, and DAG order.
        """
        errors: list[str] = []
        warnings: list[str] = []

        # 1. Load all configs
        configs = self._load_all_configs(config_dir, errors)
        if not configs:
            return CompileResult(
                success=False,
                models=0,
                errors=errors or ["No valid configs found in directory"],
            )

        # 2. Build DAG and check for cycles
        try:
            dag_order = self._build_and_validate_dag(configs)
        except CycleError as e:
            cycle = " -> ".join(e.args[1])
            errors.append(f"Circular dependency detected: {cycle}")
            return CompileResult(success=False, models=len(configs), errors=errors)

        # 3. Validate sources exist (optional)
        if not skip_source_check:
            self._validate_sources_exist(configs, errors, warnings)

        # 4. Validate SQL syntax (optional)
        if not skip_sql_check:
            self._validate_sql_syntax(configs, errors)

        # 5. Validate foreign key references
        self._validate_foreign_keys(configs, errors)

        return CompileResult(
            success=len(errors) == 0,
            models=len(configs),
            errors=errors,
            warnings=warnings,
            dag_order=dag_order,
        )

    def compile_config(
        self,
        config: TableConfig,
        skip_source_check: bool = False,
        skip_sql_check: bool = False,
    ) -> CompileResult:
        """Validate a single config.

        Args:
            config: TableConfig to validate.
            skip_source_check: Skip source table existence check.
            skip_sql_check: Skip SQL syntax validation.

        Returns:
            CompileResult with validation status.
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Validate sources exist
        if not skip_source_check:
            self._validate_sources_exist([config], errors, warnings)

        # Validate SQL syntax
        if not skip_sql_check:
            self._validate_sql_syntax([config], errors)

        return CompileResult(
            success=len(errors) == 0,
            models=1,
            errors=errors,
            warnings=warnings,
            dag_order=[config.table_name],
        )

    def _load_all_configs(
        self, config_dir: str, errors: list[str]
    ) -> list[TableConfig]:
        """Load all YAML configs from directory."""
        configs: list[TableConfig] = []

        yaml_files = glob.glob(os.path.join(config_dir, "*.yml")) + glob.glob(
            os.path.join(config_dir, "*.yaml")
        )

        for file_path in yaml_files:
            try:
                config = self._loader.load_config(file_path)
                configs.append(config)
            except ConfigurationError as e:
                errors.append(f"{os.path.basename(file_path)}: {e}")
            except Exception as e:
                errors.append(f"{os.path.basename(file_path)}: Unexpected error - {e}")

        return configs

    def _build_dependency_graph(
        self, configs: list[TableConfig]
    ) -> dict[str, set[str]]:
        """Build table dependency graph from sources.

        Returns:
            dict mapping table_name -> set of source table names
        """
        graph: dict[str, set[str]] = {}

        for config in configs:
            dependencies: set[str] = set()
            for source in config.sources:
                dependencies.add(source.name)
            graph[config.table_name] = dependencies

        return graph

    def _build_and_validate_dag(self, configs: list[TableConfig]) -> list[str]:
        """Build DAG and return topological order.

        Raises:
            CycleError: If circular dependency detected.
        """
        graph = self._build_dependency_graph(configs)

        # TopologicalSorter expects graph where key depends on values
        ts = TopologicalSorter(graph)
        return list(ts.static_order())

    def _validate_sources_exist(
        self,
        configs: list[TableConfig],
        errors: list[str],
        warnings: list[str],
    ) -> None:
        """Check all source tables exist in catalog."""
        # Build set of tables we're creating (to allow self-references)
        target_tables = {c.table_name for c in configs}

        for config in configs:
            for source in config.sources:
                # Skip if source is a target we're creating
                if source.name in target_tables:
                    continue

                try:
                    if not self.spark.catalog.tableExists(source.name):
                        errors.append(
                            f"{config.table_name}: Source table '{source.name}' not found"
                        )
                except Exception as e:
                    # Catalog access issues → warning (might be permissions)
                    warnings.append(
                        f"{config.table_name}: Could not check source '{source.name}': {e}"
                    )

    def _validate_sql_syntax(
        self, configs: list[TableConfig], errors: list[str]
    ) -> None:
        """Validate SQL syntax using EXPLAIN (dry-run)."""
        for config in configs:
            if not config.transformation_sql:
                continue

            # Build CTE-style SQL to validate references
            sql = self._build_validation_sql(config)

            try:
                # EXPLAIN parses SQL without executing
                self.spark.sql(f"EXPLAIN {sql}")
            except Exception as e:
                error_msg = str(e)
                # Extract the relevant part of the error
                if "AnalysisException" in error_msg:
                    # Clean up Spark error message
                    error_msg = error_msg.split("AnalysisException:")[-1].strip()
                errors.append(f"{config.table_name}: SQL error - {error_msg[:200]}")

    def _build_validation_sql(self, config: TableConfig) -> str:
        """Build SQL for validation with source CTEs.

        Creates CTEs for each source alias to validate the transformation SQL.
        """
        ctes = []
        for source in config.sources:
            # Create CTE that selects * from source with alias
            ctes.append(f"{source.alias} AS (SELECT * FROM {source.name})")

        if ctes:
            cte_sql = "WITH " + ",\n".join(ctes)
            return f"{cte_sql}\n{config.transformation_sql}"
        return config.transformation_sql

    def _validate_foreign_keys(
        self, configs: list[TableConfig], errors: list[str]
    ) -> None:
        """Validate FK references point to known dimension tables."""
        # Build set of all table names (targets)
        all_tables = {c.table_name for c in configs}

        for config in configs:
            if not config.foreign_keys:
                continue

            for fk in config.foreign_keys:
                if fk.references:
                    # Check if referenced table is in our config set
                    # (External tables should be checked via source validation)
                    if fk.references not in all_tables:
                        # Check if it exists in catalog
                        try:
                            if not self.spark.catalog.tableExists(fk.references):
                                errors.append(
                                    f"{config.table_name}: FK '{fk.column}' references "
                                    f"unknown table '{fk.references}'"
                                )
                        except Exception:
                            # If we can't check, just warn (handled by source check)
                            pass
