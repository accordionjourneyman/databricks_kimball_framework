import yaml
import os
from typing import Dict, Any, List
from dataclasses import dataclass
from jinja2 import Template

@dataclass
class SourceConfig:
    name: str
    alias: str
    join_on: str = None
    cdc_strategy: str = "cdf"  # cdf, full, timestamp
    primary_keys: List[str] = None  # Keys for CDF deduplication (prevents duplicate row errors)

@dataclass
class ForeignKeyConfig:
    """
    Kimball-style foreign key declaration for fact tables.
    Explicitly declares which columns are surrogate key references to dimensions.
    """
    column: str  # Column name in the fact table (e.g., 'customer_sk')
    references: str = None  # Optional: dimension table name for documentation/Bus Matrix
    default_value: int = -1  # Default value for NULL handling (-1=Unknown, -2=N/A, -3=Error)

@dataclass
class TableConfig:
    table_name: str
    table_type: str
    surrogate_key: str
    natural_keys: List[str]
    sources: List[SourceConfig]
    transformation_sql: str = None
    delete_strategy: str = "hard"
    enable_audit_columns: bool = True
    scd_type: int = 1
    track_history_columns: List[str] = None
    default_rows: Dict[str, Any] = None
    surrogate_key_strategy: str = "identity" # identity (GENERATED ALWAYS AS IDENTITY), hash, sequence
    schema_evolution: bool = False
    early_arriving_facts: List[Dict[str, str]] = None # List of {dimension_table: ..., join_key: ...}
    cluster_by: List[str] = None  # Columns for Liquid Clustering
    optimize_after_merge: bool = False  # Run OPTIMIZE after MERGE
    merge_keys: List[str] = None  # For facts: columns used in MERGE condition (degenerate dimensions)
    foreign_keys: List[ForeignKeyConfig] = None  # Kimball: explicit FK declarations for fact tables

class ConfigLoader:
    """
    Loads and validates YAML configuration files with Jinja2 templating support.
    """

    def __init__(self, env_vars: Dict[str, str] = None):
        self.env_vars = env_vars or os.environ.copy()

    def load_config(self, file_path: str) -> TableConfig:
        """
        Reads a YAML file, renders it with Jinja2 using env_vars, and parses it into a TableConfig object.
        """
        with open(file_path, 'r') as f:
            raw_content = f.read()

        # Render Jinja2 template
        template = Template(raw_content)
        rendered_content = template.render(self.env_vars)

        # Parse YAML
        config_dict = yaml.safe_load(rendered_content)

        return self._parse_dict(config_dict)

    def _parse_dict(self, config: Dict[str, Any]) -> TableConfig:
        """Validates and converts dictionary to TableConfig."""
        try:
            sources = [
                SourceConfig(
                    name=s["name"],
                    alias=s.get("alias", s["name"].split(".")[-1]),
                    join_on=s.get("join_on"),
                    cdc_strategy=s.get("cdc_strategy", "cdf"),
                    primary_keys=s.get("primary_keys")
                ) for s in config.get("sources", [])
            ]

            table_type = config.get("table_type", "fact")
            keys_cfg = config.get("keys", {}) or {}

            # Kimball: Dimensions MUST have surrogate_key and natural_keys.
            # Facts should NOT have keys; they use merge_keys (degenerate dimensions) instead.
            if table_type == "dimension":
                if not keys_cfg.get("surrogate_key"):
                    raise ValueError("Dimensions require keys.surrogate_key")
                if not keys_cfg.get("natural_keys"):
                    raise ValueError("Dimensions require keys.natural_keys")

            # For facts, use merge_keys (degenerate dimension columns for MERGE condition)
            merge_keys = config.get("merge_keys", [])

            # Parse foreign_keys for fact tables (Kimball-proper FK declarations)
            foreign_keys_raw = config.get("foreign_keys", []) or []
            foreign_keys = [
                ForeignKeyConfig(
                    column=fk["column"],
                    references=fk.get("references"),
                    default_value=fk.get("default_value", -1)
                ) for fk in foreign_keys_raw
            ]

            return TableConfig(
                table_name=config["table_name"],
                table_type=table_type,
                surrogate_key=keys_cfg.get("surrogate_key"),
                natural_keys=keys_cfg.get("natural_keys", []),
                sources=sources,
                transformation_sql=config.get("transformation_sql"),
                delete_strategy=config.get("delete_strategy", "hard"),
                enable_audit_columns=config.get("audit_columns", True),
                scd_type=config.get("scd_type", 1),
                track_history_columns=config.get("track_history_columns"),
                default_rows=config.get("default_rows"),
                surrogate_key_strategy=config.get("surrogate_key_strategy", "identity"),
                schema_evolution=config.get("schema_evolution", False),
                early_arriving_facts=config.get("early_arriving_facts"),
                cluster_by=config.get("cluster_by"),
                optimize_after_merge=config.get("optimize_after_merge", False),
                merge_keys=merge_keys,
                foreign_keys=foreign_keys if foreign_keys else None
            )
        except KeyError as e:
            raise ValueError(f"Missing required configuration field: {e}")
