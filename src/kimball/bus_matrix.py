import os
import glob
from typing import List, Dict
from kimball.config import ConfigLoader

def generate_bus_matrix(config_dir: str) -> str:
    """
    Scans a directory for YAML configs and generates an Enterprise Bus Matrix in Markdown.
    """
    loader = ConfigLoader()
    facts = []
    all_dimensions = set()
    
    # 1. Parse all configs
    yaml_files = glob.glob(os.path.join(config_dir, "*.yml")) + glob.glob(os.path.join(config_dir, "*.yaml"))
    
    matrix_data = {} # Fact -> Set(Dimensions)

    for f in yaml_files:
        try:
            config = loader.load_config(f)
            # We assume 'fact' tables are the rows
            if config.table_type == 'fact':
                facts.append(config.table_name)
                dims = set()
                for src in config.sources:
                    # Heuristic: If source name contains 'dim', it's a dimension
                    # Or we just list all sources.
                    # Ideally, we filter for dimensions.
                    if "dim" in src.name.lower():
                        dims.add(src.name)
                        all_dimensions.add(src.name)
                matrix_data[config.table_name] = dims
        except Exception as e:
            print(f"Skipping {f}: {e}")

    # 2. Sort for consistency
    sorted_facts = sorted(facts)
    sorted_dims = sorted(list(all_dimensions))

    # 3. Build Markdown Table
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
