# Kimball Framework SDK Expansion Ideas

This document outlines potential enhancements to expose more of the Kimball framework's internal components as a public SDK, enabling advanced users to customize and extend the ETL pipeline.

---

## Motivation

Currently, only `Orchestrator` and `generate_bus_matrix` are exported from the `kimball` package. Power users may want to:

- Use individual components (e.g., `DeltaMerger`, `DataLoader`) standalone
- Build custom pipelines with fine-grained control
- Extend or subclass components for specialized behavior
- Integrate with external orchestration tools (Airflow, Dagster, Prefect)

---

## Proposed Public API Additions

| Component            | Module                    | Use Case                                                                 |
|----------------------|---------------------------|--------------------------------------------------------------------------|
| `DeltaMerger`        | `kimball.merger`          | Custom merge strategies, SCD Type 1/2/3 variations                       |
| `DataLoader`         | `kimball.loader`          | Load data from custom sources, apply pre-load transformations            |
| `WatermarkManager`   | `kimball.watermark`       | Manual watermark manipulation, custom checkpoint strategies              |
| `SkeletonGenerator`  | `kimball.skeleton_generator` | Early-arriving fact handling, placeholder dimension record creation   |
| `ConfigLoader`       | `kimball.config`          | Programmatic config generation, validation, schema introspection         |
| `TableConfig`        | `kimball.config`          | Build configs in code instead of YAML                                    |
| `KeyGenerator`       | `kimball.key_generator`   | Custom surrogate key generation strategies                               |
| `Hasher`             | `kimball.hashing`         | Row hashing for change detection, custom hash algorithms                 |

---

## Implementation Plan

### Phase 1: Expand `__init__.py` Exports

```python
# src/kimball/__init__.py
from kimball.orchestrator import Orchestrator
from kimball.bus_matrix import generate_bus_matrix
from kimball.merger import DeltaMerger
from kimball.loader import DataLoader
from kimball.watermark import WatermarkManager
from kimball.skeleton_generator import SkeletonGenerator
from kimball.config import ConfigLoader, TableConfig
from kimball.key_generator import KeyGenerator
from kimball.hashing import Hasher

__all__ = [
    "Orchestrator",
    "generate_bus_matrix",
    "DeltaMerger",
    "DataLoader",
    "WatermarkManager",
    "SkeletonGenerator",
    "ConfigLoader",
    "TableConfig",
    "KeyGenerator",
    "Hasher",
]
```

### Phase 2: Add Type Hints & Docstrings

Ensure all public classes and methods have:
- Complete type annotations (PEP 484)
- Docstrings with usage examples
- Parameter/return descriptions

### Phase 3: Create Usage Examples

Add example scripts demonstrating:

1. **Custom Merge Pipeline**
   ```python
   from kimball import DeltaMerger, DataLoader, ConfigLoader

   config = ConfigLoader("dim_customer.yml").load()
   loader = DataLoader(spark)
   merger = DeltaMerger(spark)

   df = loader.load_cdf(config.source_table, watermark)
   merger.merge_scd2(df, config)
   ```

2. **Programmatic Config Generation**
   ```python
   from kimball import TableConfig, Orchestrator

   config = TableConfig(
       table_name="dim_product",
       table_type="dimension",
       scd_type=2,
       business_keys=["product_id"],
       tracked_columns=["product_name", "category", "price"],
   )
   Orchestrator(config).run()
   ```

3. **Custom Watermark Strategy**
   ```python
   from kimball import WatermarkManager

   wm = WatermarkManager(spark, "custom_watermarks")
   wm.set_watermark("my_table", "2024-01-01T00:00:00")
   current = wm.get_watermark("my_table")
   ```

### Phase 4: Plugin Architecture (Future)

Allow users to register custom implementations:

```python
from kimball import Orchestrator
from kimball.plugins import register_merger

@register_merger("custom_scd6")
class SCD6Merger:
    def merge(self, df, config):
        # Custom SCD Type 6 logic
        pass

# Use in config
# merge_strategy: custom_scd6
```

---

## Use Cases

### 1. Multi-Source Fact Tables
Load from multiple sources, apply custom joins, then merge:
```python
orders = loader.load_cdf("bronze.orders", wm1)
items = loader.load_cdf("bronze.order_items", wm2)
fact_df = orders.join(items, "order_id")
merger.merge_fact(fact_df, fact_config)
```

### 2. External Orchestration Integration
Use with Airflow/Dagster for enterprise scheduling:
```python
# Airflow DAG
@task
def load_dim_customer():
    from kimball import Orchestrator
    Orchestrator("/configs/dim_customer.yml").run()
```

### 3. Testing & Validation
Unit test individual components:
```python
def test_scd2_merge():
    merger = DeltaMerger(spark)
    result = merger.merge_scd2(test_df, test_config)
    assert result.filter("is_current = true").count() == expected
```

### 4. Custom Key Generation
Use UUIDs, sequences, or composite keys:
```python
from kimball import KeyGenerator

keygen = KeyGenerator(strategy="uuid")
df_with_keys = keygen.generate(df, "surrogate_key")
```

---

## Next Steps

1. [ ] Review and prioritize components for Phase 1
2. [ ] Add comprehensive type hints to all modules
3. [ ] Write docstrings with examples
4. [ ] Create example notebooks for each use case
5. [ ] Update README with SDK documentation
6. [ ] Consider versioning strategy for API stability
