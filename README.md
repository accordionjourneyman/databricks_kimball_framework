# Databricks Kimball Framework

A declarative, production-ready framework for building Kimball-style data warehouses on Databricks using Delta Lake.

## Release 0.1.1 (2025-12-03)

- Short summary: fixes for imports, SCD merge behavior (SCD1 & SCD2), config parsing improvements, schema-evolution handling, Delta identity surrogate-key support, watermark and CDF ingestion fixes. See `CHANGELOG.md` for full details and migration notes.

## Features

- **Declarative YAML Configuration** - Define dimensions and facts without writing boilerplate code
- **SCD Type 1 & Type 2** - Full support for slowly changing dimensions with multiple surrogate key strategies
- **Change Data Feed (CDF) Integration** - Efficient incremental processing using Delta Lake CDF
- **Watermark Management** - Automatic tracking of processed data versions for exactly-once semantics
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

1. **Define a Dimension** (`configs/dim_customer.yml`):

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

2. **Run the Pipeline**:

```python
from kimball.orchestrator import Orchestrator

orchestrator = Orchestrator("configs/dim_customer.yml")
orchestrator.run()
```

## Architecture

```
┌─────────────────┐
│  YAML Config    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│  Orchestrator   │─────▶│  Watermark   │
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
surrogate_key_strategy: hash  # or: identity, sequence
track_history_columns: [name, category, price]
```

#### Fact Table
```yaml
table_type: fact
# Facts do not define surrogate or natural keys. Instead, provide
# `merge_keys` which are the degenerate key columns used in the MERGE
# condition (e.g., order_item_id for fact lines).
merge_keys: [order_item_id]
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

### Watermark Management

The framework maintains a `kimball_watermarks` table tracking:
- `target_table` - The table being loaded
- `source_table` - The source being read
- `last_processed_version` - Last Delta version processed

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

### Watermark not advancing
Check that the source table has new versions: `DESCRIBE HISTORY source_table`

### Duplicate keys in SCD2
Verify `natural_keys` uniquely identify business entities

## License

Licensed under the Apache License, Version 2.0. See the `LICENSE` file for details.
