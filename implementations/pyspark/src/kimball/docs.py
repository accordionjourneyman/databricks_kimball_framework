"""
Documentation Generator - Auto-generate catalog and lineage docs from Kimball configs.

Provides:
- Manifest generation (model metadata as JSON)
- Mermaid DAG visualization
- Markdown catalog generation
- Bus matrix enhancement

Usage:
    from kimball.docs import CatalogGenerator

    generator = CatalogGenerator()
    manifest = generator.generate_manifest(configs)
    dag = generator.generate_lineage_dag(configs)
    catalog = generator.generate_catalog(configs)
"""

from __future__ import annotations

import glob
import json
import os
from dataclasses import asdict, dataclass, field
from typing import Any

from kimball.config import ConfigLoader, TableConfig


@dataclass
class ModelMetadata:
    """Metadata for a single model."""

    name: str
    table_type: str
    sources: list[str]
    columns: list[str] = field(default_factory=list)
    description: str | None = None
    tests: list[str] = field(default_factory=list)
    foreign_keys: list[str] = field(default_factory=list)


class CatalogGenerator:
    """Generate documentation from Kimball configs."""

    def __init__(self, config_loader: ConfigLoader | None = None):
        """Initialize generator.

        Args:
            config_loader: ConfigLoader instance. If None, creates a new one.
        """
        self.loader = config_loader or ConfigLoader()

    def load_configs(self, config_dir: str) -> list[TableConfig]:
        """Load all configs from a directory."""
        configs: list[TableConfig] = []
        yaml_files = glob.glob(os.path.join(config_dir, "*.yml")) + glob.glob(
            os.path.join(config_dir, "*.yaml")
        )
        for file_path in yaml_files:
            try:
                config = self.loader.load_config(file_path)
                configs.append(config)
            except Exception:
                pass  # Skip invalid configs
        return configs

    def generate_manifest(self, configs: list[TableConfig]) -> dict[str, Any]:
        """Create manifest.json with all model metadata.

        Args:
            configs: List of TableConfig objects.

        Returns:
            Dict with metadata for all models, sources, and relationships.
        """
        manifest: dict[str, Any] = {
            "metadata": {
                "generator": "kimball-framework",
                "schema_version": "1.0",
            },
            "models": {},
            "sources": set(),
            "relationships": [],
        }

        for config in configs:
            # Extract source names
            source_names = [s.name for s in config.sources]
            manifest["sources"].update(source_names)

            # Extract test names
            test_names = []
            if config.tests:
                for test_def in config.tests:
                    for test in test_def.tests:
                        if isinstance(test, str):
                            test_names.append(f"{test_def.column}:{test}")
                        elif isinstance(test, dict):
                            test_type = list(test.keys())[0]
                            test_names.append(f"{test_def.column}:{test_type}")

            # Extract FK references
            fk_refs = []
            if config.foreign_keys:
                for fk in config.foreign_keys:
                    if fk.references:
                        fk_refs.append(f"{fk.column} -> {fk.references}")
                        manifest["relationships"].append(
                            {
                                "from": config.table_name,
                                "to": fk.references,
                                "column": fk.column,
                            }
                        )

            model = ModelMetadata(
                name=config.table_name,
                table_type=config.table_type,
                sources=source_names,
                tests=test_names,
                foreign_keys=fk_refs,
            )
            manifest["models"][config.table_name] = asdict(model)

        # Convert set to sorted list
        manifest["sources"] = sorted(manifest["sources"])

        return manifest

    def generate_lineage_dag(self, configs: list[TableConfig]) -> str:
        """Create Mermaid syntax DAG visualization.

        Args:
            configs: List of TableConfig objects.

        Returns:
            Mermaid graph definition string.
        """
        lines = ["```mermaid", "graph LR"]

        # Build nodes and edges
        for config in configs:
            target = self._sanitize_node_name(config.table_name)

            for source in config.sources:
                source_node = self._sanitize_node_name(source.name)
                lines.append(f"    {source_node} --> {target}")

        lines.append("```")
        return "\n".join(lines)

    def _sanitize_node_name(self, name: str) -> str:
        """Sanitize table name for Mermaid node ID."""
        # Replace dots with underscores for valid Mermaid IDs
        return name.replace(".", "_").replace("-", "_")

    def generate_catalog(self, configs: list[TableConfig]) -> str:
        """Create Markdown catalog with table/column docs.

        Args:
            configs: List of TableConfig objects.

        Returns:
            Markdown formatted catalog string.
        """
        lines = ["# Data Catalog", "", "Generated from Kimball Framework configs.", ""]

        # Group by type
        dimensions = [c for c in configs if c.table_type == "dimension"]
        facts = [c for c in configs if c.table_type == "fact"]

        if dimensions:
            lines.extend(["## Dimensions", ""])
            for config in sorted(dimensions, key=lambda c: c.table_name):
                lines.extend(self._format_table_doc(config))

        if facts:
            lines.extend(["## Fact Tables", ""])
            for config in sorted(facts, key=lambda c: c.table_name):
                lines.extend(self._format_table_doc(config))

        return "\n".join(lines)

    def _format_table_doc(self, config: TableConfig) -> list[str]:
        """Format documentation for a single table."""
        lines = [f"### {config.table_name}", ""]

        # Key info
        if config.table_type == "dimension":
            lines.append(f"- **Surrogate Key**: `{config.surrogate_key}`")
            lines.append(f"- **Natural Keys**: `{', '.join(config.natural_keys)}`")
            lines.append(f"- **SCD Type**: {config.scd_type}")
        else:
            if config.merge_keys:
                lines.append(f"- **Merge Keys**: `{', '.join(config.merge_keys)}`")

        # Sources
        source_names = [s.name for s in config.sources]
        lines.append(f"- **Sources**: {', '.join(source_names)}")

        # Foreign keys
        if config.foreign_keys:
            fk_list = [
                f"`{fk.column}` → `{fk.references}`"
                for fk in config.foreign_keys
                if fk.references
            ]
            if fk_list:
                lines.append(f"- **Foreign Keys**: {', '.join(fk_list)}")

        # Tests
        if config.tests:
            test_count = sum(len(t.tests) for t in config.tests)
            lines.append(f"- **Tests**: {test_count} defined")

        lines.append("")
        return lines

    def save_manifest(self, configs: list[TableConfig], output_path: str) -> None:
        """Save manifest to JSON file."""
        manifest = self.generate_manifest(configs)
        with open(output_path, "w") as f:
            json.dump(manifest, f, indent=2)

    def save_catalog(self, configs: list[TableConfig], output_path: str) -> None:
        """Save catalog to Markdown file."""
        catalog = self.generate_catalog(configs)
        with open(output_path, "w") as f:
            f.write(catalog)
