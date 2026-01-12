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
    primary_keys: [customer_id]  # Required for CDF deduplication

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

> [!WARNING] > **SCD2 Intra-Batch Limitation**: When multiple changes for the same natural key occur within a single batch (between watermarks), only the latest version is preserved. Earlier intermediate changes are collapsed. For complete history, ensure batch intervals are smaller than your change frequency.

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

| Strategy   | Description                 | Use Case                              |
| ---------- | --------------------------- | ------------------------------------- |
| `identity` | Databricks Identity Columns | New tables, best performance          |
| `hash`     | xxhash64 of natural keys    | Idempotency, distributed systems      |
| `sequence` | Row number + max key        | Legacy compatibility (use cautiously) |

### track_history_columns

Columns that trigger new SCD2 versions when changed.

**Best Practice:** Only track attributes that:

1. Change over time
2. Matter for historical analysis

**Example:**

```yaml
track_history_columns:
  - title # Track promotions
  - department # Track transfers
  - salary # Track raises
# Don't track: last_login, updated_at (too volatile)
```

### effective_at

Column containing the business effective date for SCD2 history tracking.

**Time Semantics:**

- If set: Uses the specified column value for `__valid_from` and `__valid_to` boundaries (business time)
- If not set: Uses `__etl_processed_at` (processing time)

**Use Cases:**
| Scenario | effective_at | Result |
|----------|--------------|--------|
| Real-time source | Not set | Processing time is acceptable |
| Batch from source system | `updated_at` | Event time from source |
| Historical data load | `effective_date` | Business effective date |

**Example:**

```yaml
# Business-time SCD2: history reflects when changes actually occurred
effective_at: updated_at
# Processing-time SCD2 (default): history reflects when changes were processed
# effective_at: (omit or set to null)
```

> [!NOTE]
> For accurate temporal queries, use `effective_at` when the source provides reliable event timestamps.

### cdc_strategy

How to read source data.

**Options:**

| Strategy | Description                    | When to Use                    |
| -------- | ------------------------------ | ------------------------------ |
| `cdf`    | Change Data Feed (incremental) | Large tables, frequent updates |
| `full`   | Full table snapshot            | Small dimensions, fact lookups |

### primary_keys

**Required for CDF sources.** List of columns that uniquely identify a row in the source table.

Used for **deduplication** when a single CDF batch contains multiple operations on the same row (e.g., insert then update). Without this, you may get duplicate rows.

**Example:**

```yaml
sources:
  - name: silver.customers
    alias: c
    cdc_strategy: cdf
    primary_keys: [customer_id] # Single key

  - name: silver.order_items
    alias: oi
    cdc_strategy: cdf
    primary_keys: [order_id, line_number] # Composite key
```

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

## Performance Features (Feature Flags)

The following environment variables control performance optimizations and validation levels:

| Environment Variable                | Default        | Description                                                                                                                                                   |
| ----------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KIMBALL_ENABLE_DEV_CHECKS`         | `0` (Disabled) | **Strict Mode**. Enables expensive data quality counts (validation) and pre-merge grain checks (merger). Recommended for Dev/Test only.                       |
| `KIMBALL_ENABLE_INLINE_OPTIMIZE`    | `0` (Disabled) | Enables `OPTIMIZE` command immediately after every merge. Not recommended for high-frequency or streaming jobs (adds latency). Use async maintenance instead. |
| `KIMBALL_ALLOW_UNSAFE_SEQUENCE_KEY` | `0` (Blocked)  | **Dangerous**. Overrides the blockage of `SequenceKeyGenerator`. Setting this to `1` may cause OOM errors on large datasets due to global sort.               |
