import glob
import os

from kimball.common.config import ConfigLoader, TableConfig

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
            print(f"Skipping {f}: {e}")
    return configs


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
    Uses a pipeline of Parse -> Analyze -> Render.
    """
    configs = parse_configs(config_dir)
    sorted_facts, matrix_data, sorted_dims = analyze_dependencies(configs)
    return render_markdown(sorted_facts, matrix_data, sorted_dims)
