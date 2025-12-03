# Configuration Guide

Complete reference for YAML configuration files.

## Table Configuration Structure

```yaml
# Required: Target table name
table_name: {{ env }}_gold.dim_customer

# Required: Table type
table_type: dimension  # or: fact

# Required for dimensions: SCD type
scd_type: 2  # or: 1

# Required: Key definitions
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]  # Can be composite: [region, customer_id]

# Optional: Surrogate key generation strategy (default: identity)
surrogate_key_strategy: hash  # Options: identity, hash, sequence

# Required for SCD2: Columns that trigger new versions
track_history_columns:
  - first_name
  - last_name
  - email
  - address

# Optional: Default/unknown rows to seed
default_rows:
  -1: "Unknown"
  -2: "N/A"

# Optional: Enable automatic schema evolution (default: false)
schema_evolution: true

# Optional: Early arriving facts configuration (facts only)
early_arriving_facts:
  - dimension_table: {{ env }}_gold.dim_customer
    fact_join_key: customer_id
    dimension_join_key: customer_id
    surrogate_key_col: customer_sk
    surrogate_key_strategy: hash

# Required: Source tables
sources:
  - name: {{ env }}_silver.customers
    alias: c
    cdc_strategy: cdf  # or: full

# Optional: Custom transformation SQL
transformation_sql: |
  SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.address
  FROM c

# Optional: Enable audit columns (default: false)
audit_columns: true

# Optional: Delete strategy for SCD1 (default: hard)
delete_strategy: hard  # or: soft
```

## Field Descriptions

### table_name
Full table name including catalog and schema. Supports Jinja2 templating.

**Example:**
```yaml
table_name: {{ env }}_gold.dim_customer
```

### table_type
Type of table being loaded.

**Options:**
- `dimension` - Dimension table
- `fact` - Fact table

### scd_type
Slowly Changing Dimension type (dimensions only).

**Options:**
- `1` - Overwrite changes (no history)
- `2` - Track full history with versioning

### keys.surrogate_key
Name of the surrogate key column.

**Convention:** `<table>_sk`

### keys.natural_keys
List of business key columns that uniquely identify an entity.

**Examples:**
```yaml
# Single key
natural_keys: [customer_id]

# Composite key
natural_keys: [region_id, customer_id]
```

### surrogate_key_strategy
Method for generating surrogate keys.

**Options:**

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `identity` | Databricks Identity Columns | New tables, best performance |
| `hash` | xxhash64 of natural keys | Idempotency, distributed systems |
| `sequence` | Row number + max key | Legacy compatibility (use cautiously) |

### track_history_columns
Columns that trigger new SCD2 versions when changed.

**Best Practice:** Only track attributes that:
1. Change over time
2. Matter for historical analysis

**Example:**
```yaml
track_history_columns:
  - title        # Track promotions
  - department   # Track transfers
  - salary       # Track raises
# Don't track: last_login, updated_at (too volatile)
```

### cdc_strategy
How to read source data.

**Options:**

| Strategy | Description | When to Use |
|----------|-------------|-------------|
| `cdf` | Change Data Feed (incremental) | Large tables, frequent updates |
| `full` | Full table snapshot | Small dimensions, fact lookups |

### transformation_sql
Spark SQL to transform source data.

**Features:**
- Use source aliases defined in `sources`
- Standard Spark SQL syntax
- CDF metadata flows through automatically

**Example:**
```yaml
transformation_sql: |
  SELECT
    oi.order_item_id,
    o.order_date,
    c.customer_sk,
    oi.quantity * p.unit_price as amount
  FROM oi
  JOIN o ON oi.order_id = o.order_id
  LEFT JOIN c ON o.customer_id = c.customer_id
    AND o.order_date >= c.__valid_from
    AND (o.order_date < c.__valid_to)
  LEFT JOIN p ON oi.product_id = p.product_id
```

### early_arriving_facts
Configuration for handling facts that arrive before dimension data.

**Fields:**
- `dimension_table` - Target dimension table
- `fact_join_key` - Column in fact source
- `dimension_join_key` - Column in dimension
- `surrogate_key_col` - Dimension's surrogate key column
- `surrogate_key_strategy` - How to generate skeleton keys

**Example:**
```yaml
early_arriving_facts:
  - dimension_table: prod_gold.dim_employee
    fact_join_key: employee_id
    dimension_join_key: employee_id
    surrogate_key_col: employee_sk
    surrogate_key_strategy: hash
```

## Standard Columns

The framework automatically manages these columns:

### SCD2 Columns (scd_type: 2)
- `__valid_from` - Row effective timestamp
- `__valid_to` - Row expiration timestamp (NULL = current)
- `__is_current` - Boolean flag (true = current version)
- `hashdiff` - Hash of tracked columns

### Audit Columns (audit_columns: true)
- `__etl_batch_id` - UUID of pipeline run
- `__etl_processed_at` - Timestamp of processing

### SCD1 Soft Delete (delete_strategy: soft)
- `__is_deleted` - Boolean flag for soft-deleted rows

## Environment Variables

Use Jinja2 templating for environment-specific values:

```yaml
table_name: {{ env }}_gold.dim_customer
sources:
  - name: {{ env }}_silver.customers
```

Set the `env` variable before running:
```python
from jinja2 import Template

config_template = Template(open("config.yml").read())
config_yaml = config_template.render(env="prod")
```

## Complete Examples

See `examples/configs/` for:
- `dim_customer.yml` - SCD2 with hash keys
- `dim_employee_scd2_identity.yml` - SCD2 with identity columns
- `dim_product.yml` - SCD1 dimension
- `fact_sales.yml` - Fact with early arriving facts
