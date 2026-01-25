import glob
import logging
import os
from collections import defaultdict

from kimball.common.config import ConfigLoader, TableConfig

logger = logging.getLogger(__name__)

# FactName -> Set[DimName]
MatrixData = dict[str, set[str]]


def parse_configs(config_dir: str) -> list[TableConfig]:
    """Parse all YAML configs in the directory."""
    loader = ConfigLoader()
    configs = []
    yaml_files = glob.glob(os.path.join(config_dir, "*.yml")) + glob.glob(
        os.path.join(config_dir, "*.yaml")
    )

    for f in yaml_files:
        try:
            config = loader.load_config(f)
            configs.append(config)
        except Exception as e:
            logger.warning(f"Skipping {f}: config load failed - {e}")
    return configs


def validate_conformed_dimensions(
    configs: list[TableConfig],
) -> list[str]:
    """
    FINDING-022: Validate that dimensions used by multiple facts are truly conformed.

    Identifies dimensions referenced by multiple fact tables and warns if they
    reference different physical tables (indicating non-conformity).

    Args:
        configs: List of parsed TableConfig objects.

    Returns:
        List of warning messages for non-conformed dimensions.
    """
    warnings = []

    # Track dimension references: dim_name -> set of (fact_name, actual_table)
    dim_references: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for config in configs:
        if config.table_type == "fact" and config.foreign_keys:
            for fk in config.foreign_keys:
                if fk.references:
                    # Normalize dimension name for comparison
                    dim_short_name = fk.references.split(".")[-1]
                    dim_references[dim_short_name].append(
                        (config.table_name, fk.references)
                    )

    # Check for non-conformity (same logical dimension, different physical tables)
    for dim_name, refs in dim_references.items():
        if len(refs) > 1:
            # Get unique physical tables
            physical_tables = set(ref[1] for ref in refs)
            if len(physical_tables) > 1:
                fact_names = [ref[0] for ref in refs]
                warnings.append(
                    f"NON-CONFORMED DIMENSION WARNING: '{dim_name}' is referenced by "
                    f"multiple facts ({', '.join(fact_names)}) but uses different physical tables: "
                    f"{', '.join(physical_tables)}. "
                    f"Kimball methodology requires conformed dimensions to use the same table."
                )

    return warnings


def analyze_dependencies(
    configs: list[TableConfig],
) -> tuple[list[str], MatrixData, list[str]]:
    """
    Analyze dependencies to build the bus matrix.
    Returns: (sorted_facts, matrix_data, sorted_dims)
    """
    known_dimensions = {c.table_name for c in configs if c.table_type == "dimension"}
    facts = []
    matrix_data: MatrixData = {}
    used_dimensions = set()

    for config in configs:
        if config.table_type == "fact":
            facts.append(config.table_name)
            dims = set()

            # Strategy A: Explicit Foreign Keys
            if config.foreign_keys:
                for fk in config.foreign_keys:
                    if fk.references:
                        dims.add(fk.references)

            # Strategy B: Source Dependency
            for src in config.sources:
                if src.name in known_dimensions:
                    dims.add(src.name)
                elif "dim" in src.name.lower():
                    dims.add(src.name)

            matrix_data[config.table_name] = dims
            used_dimensions.update(dims)

    sorted_facts = sorted(facts)
    sorted_dims = sorted(list(used_dimensions.union(known_dimensions)))
    return sorted_facts, matrix_data, sorted_dims


def render_markdown(
    sorted_facts: list[str], matrix_data: MatrixData, sorted_dims: list[str]
) -> str:
    """Render the bus matrix as a Markdown table."""
    if not sorted_dims:
        return "No dimensions found."

    # Header
    md = "| Business Process (Fact) | " + " | ".join(sorted_dims) + " |\n"
    md += "| --- | " + " | ".join(["---"] * len(sorted_dims)) + " |\n"

    # Rows
    for fact in sorted_facts:
        row = f"| {fact} |"
        fact_dims = matrix_data.get(fact, set())
        for dim in sorted_dims:
            if dim in fact_dims:
                row += " X |"
            else:
                row += "   |"
        md += row + "\n"

    return md


def generate_bus_matrix(config_dir: str) -> str:
    """
    Scans a directory for YAML configs and generates an Enterprise Bus Matrix in Markdown.
    Uses a pipeline of Parse -> Analyze -> Validate -> Render.

    FINDING-022: Now includes validation for conformed dimensions.
    """
    configs = parse_configs(config_dir)

    # Validate conformed dimensions and print warnings
    conformity_warnings = validate_conformed_dimensions(configs)
    for warning in conformity_warnings:
        logger.warning(warning)

    sorted_facts, matrix_data, sorted_dims = analyze_dependencies(configs)
    return render_markdown(sorted_facts, matrix_data, sorted_dims)
