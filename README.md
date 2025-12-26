# Databricks Kimball Framework

A declarative, production-ready framework for building Kimball-style data warehouses on Databricks using Delta Lake.

## Release 0.1.1 (2025-01-06)

- **Renamed `WatermarkManager` → `ETLControlManager`** - Better reflects Kimball-style ETL auditing role.
- **`KIMBALL_ETL_SCHEMA` environment variable** - Set once at notebook start, no need to pass to every Orchestrator.
- See `CHANGELOG.md` for full details.

## Features

- **Declarative YAML Configuration** - Define dimensions and facts without writing boilerplate code
- **SCD Type 1 & Type 2** - Full support for slowly changing dimensions with multiple surrogate key strategies
- **Change Data Feed (CDF) Integration** - Efficient incremental processing using Delta Lake CDF
- **ETL Control Table** - Kimball-style batch auditing with watermarks, lifecycle tracking, and metrics
- **Parallel Execution** - Wave-based parallel pipelines (dimensions before facts)
- **Early Arriving Facts** - Automatic skeleton dimension row generation
- **Schema Evolution** - Opt-in automatic schema merging
- **Liquid Clustering** - Automatic table optimization with configurable clustering columns
- **Audit Columns** - Built-in ETL lineage tracking

## Quick Start

### Installation

```bash
pip install -e .
```

### Basic Usage

1. **Configure ETL Schema** (set once at notebook start):

```python
import os
os.environ["KIMBALL_ETL_SCHEMA"] = "prod_gold"  # Where to store ETL control table
```

2. **Define a Dimension** (`configs/dim_customer.yml`):

```yaml
table_name: prod_gold.dim_customer
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]

surrogate_key_strategy: hash
track_history_columns: [first_name, last_name, email, address]

sources:
  - name: prod_silver.customers
    alias: c
    cdc_strategy: cdf
    primary_keys: [customer_id]  # Required for CDF deduplication

transformation_sql: |
  SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.address
  FROM c

audit_columns: true
```

3. **Run the Pipeline**:

```python
from kimball import Orchestrator

# Configure checkpoint directory for reliable DataFrame checkpointing
orchestrator = Orchestrator(
    config_path="configs/dim_customer.yml",
    checkpoint_root="dbfs:/kimball/checkpoints/"  # Required for production reliability
)
orchestrator.run()
```

**Environment Variables** (set once at notebook/cluster start):

```python
import os
os.environ["KIMBALL_ETL_SCHEMA"] = "prod_gold"  # ETL control table location
os.environ["KIMBALL_CHECKPOINT_ROOT"] = "dbfs:/kimball/checkpoints/"  # DataFrame checkpoints
os.environ["KIMBALL_CLEANUP_REGISTRY_TABLE"] = "prod_gold.kimball_staging_registry"  # Staging cleanup registry
os.environ["KIMBALL_CHECKPOINT_TABLE"] = "prod_gold.kimball_pipeline_checkpoints"  # Pipeline checkpoints
```

## Architecture

```
┌─────────────────┐
│  YAML Config    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│  Orchestrator   │─────▶│ ETLControl   │
└────────┬────────┘      │   Manager    │
         │               └──────────────┘
         ▼
┌─────────────────┐
│   DataLoader    │ (CDF/Snapshot)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Transformation  │ (Spark SQL)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  DeltaMerger    │ (SCD1/SCD2)
└─────────────────┘
```

## Configuration Reference

### Configuration Validation

YAML configurations are automatically validated against a JSON Schema to catch configuration errors early. The validation ensures:

- Required fields are present (`table_name`, `table_type`, `sources`)
- Field types match expected formats (strings, arrays, booleans, etc.)
- Enum values are valid (e.g., `table_type` must be "dimension" or "fact")
- Dimensions require `keys.surrogate_key` and `keys.natural_keys`
- Sources have required `name` field
- CDC strategies are valid ("cdf", "full", "timestamp")

Invalid configurations will raise descriptive `ValueError` exceptions at load time.

### Table Types

#### Dimension (SCD Type 1)

```yaml
table_type: dimension
scd_type: 1
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
```

#### Dimension (SCD Type 2)

```yaml
table_type: dimension
scd_type: 2
surrogate_key_strategy: hash # identity (recommended), hash
track_history_columns: [name, category, price]
```

#### Fact Table

```yaml
table_type: fact
# Facts do not define surrogate or natural keys. Instead, provide
# `merge_keys` which are the degenerate key columns used in the MERGE
# condition (e.g., order_item_id for fact lines).
merge_keys: [order_item_id]

# Kimball-proper: Explicit foreign key declarations
# Replaces the old naming convention hack (columns ending with '_sk')
foreign_keys:
  - column: customer_sk
    references: prod_gold.dim_customer # Optional: for documentation/Bus Matrix
    default_value: -1 # Default for NULL handling (-1=Unknown, -2=N/A, -3=Error)
  - column: product_sk
    references: prod_gold.dim_product

schema_evolution: true
early_arriving_facts:
  - dimension_table: prod_gold.dim_customer
    fact_join_key: customer_id
    dimension_join_key: customer_id
    surrogate_key_col: customer_sk
    surrogate_key_strategy: hash
```

### CDC Strategies

- **`cdf`** - Incremental processing using Delta Change Data Feed
- **`full`** - Full table snapshot (for dimension lookups in facts)

## Key Concepts

### ETL Control Table

The framework maintains an `etl_control` table tracking:

- `target_table` - The table being loaded
- `source_table` - The source being read
- `last_processed_version` - Last Delta version processed (watermark)
- `batch_id`, `batch_status` - Lifecycle tracking (RUNNING/SUCCESS/FAILED)
- `rows_read`, `rows_written` - Row metrics
- `batch_started_at`, `batch_completed_at` - Timing

Configure once at notebook start:
```python
import os
os.environ["KIMBALL_ETL_SCHEMA"] = "gold"  # Where to store ETL control table
```

This ensures exactly-once processing even across failures.

### SCD Type 2 Implementation

Uses the "Union Approach" for atomic SCD2 updates:

1. Compute `hashdiff` for change detection
2. Duplicate changed rows (one for UPDATE, one for INSERT)
3. Execute single MERGE with conditional logic

Standard SCD2 columns:

- `__valid_from` - Row effective date
- `__valid_to` - Row expiration date (NULL for current)
- `__is_current` - Boolean flag for current version
- `hashdiff` - Hash of tracked columns

### Surrogate Key Strategies

- **Identity** - Databricks Identity Columns (recommended)
- **Hash** - xxhash64 of natural keys (deterministic)
- **Sequence** - Row number + max key (use with caution)

## Examples

See the `examples/` directory for complete working examples:

- `dim_customer.yml` - SCD2 dimension with hash keys
- `dim_employee_scd2_identity.yml` - SCD2 with identity columns
- `dim_product.yml` - SCD1 dimension
- `fact_sales.yml` - Fact table with early arriving facts

## Testing

```bash
# Run unit tests
pytest tests/unit/

# Run with coverage
pytest --cov=kimball tests/
```

## Best Practices

1. **Use CDF for large tables** - Dramatically reduces processing time
2. **Enable schema evolution cautiously** - Only in dev/staging initially
3. **Choose appropriate surrogate key strategy**:
   - Identity: Best for new tables
   - Hash: Best for idempotency and distributed systems
   - Sequence: Avoid in concurrent scenarios
4. **Track minimal history columns** - Only track attributes that change and matter for analysis
5. **Use early arriving facts** - Preserves referential integrity and enables better reporting

## Troubleshooting

### "No module named 'kimball'"

Install in editable mode: `pip install -e .`

### ETL control table not found

Set the `KIMBALL_ETL_SCHEMA` environment variable:
```python
import os
os.environ["KIMBALL_ETL_SCHEMA"] = "gold"
```

### Watermark not advancing

Check that the source table has new versions: `DESCRIBE HISTORY source_table`

### Duplicate keys in SCD2

Verify `natural_keys` uniquely identify business entities

## License

Licensed under the Apache License, Version 2.0. See the `LICENSE` file for details.
