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

# Surrogate keys are generated via xxhash64(natural_keys) — deterministic and distributed-safe.
# For SCD2, __valid_from is included in the hash so each version gets a unique SK.
# No configuration needed — the framework handles this automatically.

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

> [!NOTE]
> **SCD2 Multi-Version Batches**: When multiple changes for the same natural key
> occur within a single batch, the framework preserves ALL versions using a
> two-phase algorithm (phase 1 merges the latest version, phase 2 back-fills
> intermediate history rows). Set `preserve_all_changes: true` to enable this.

### merge_keys (facts only)

**Required for fact tables.** List of "degenerate dimension" columns that
uniquely identify a fact row and serve as the MERGE condition. Facts do not
have surrogate keys (per Kimball design) — `merge_keys` is the equivalent of
`keys.natural_keys` for dimensions.

```yaml
table_type: fact
merge_keys: [order_item_id]         # single key
# or
merge_keys: [order_id, line_number] # composite key
```

Validation: a `ValueError` is raised at config load time if `table_type: fact`
without `merge_keys`.

### append_only (facts only)

When `true`, the fact table is populated with pure `INSERT INTO` instead of
`MERGE`. Use this for immutable event logs, append-only time-series, or any
source where rows are never updated or deleted. The framework skips the
expensive `MERGE` (match/upsert) and just appends new rows.

```yaml
table_type: fact
merge_keys: [order_item_id]
append_only: true
```

Validation:

- `append_only: true` requires `table_type: fact` (rejected on dimensions).
- Combine with a source using `cdc_strategy: append` for end-to-end
  incremental append without dedup overhead.

### cdc_strategy: append

The `append` strategy is a CDF read that drops `_change_type`,
`_commit_version`, and `_commit_timestamp` from the loaded DataFrame so the
downstream merge does not attempt dedup or delete handling. It is intended
to be combined with `append_only: true` on the fact table for pure
incremental append.

```yaml
sources:
  - name: silver.events
    alias: e
    cdc_strategy: append
    primary_keys: [event_id]

table_type: fact
merge_keys: [event_id]
append_only: true
```

Validation: `cdc_strategy: append` requires `append_only: true` on the
target table.

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

### Surrogate Keys

Surrogate keys are generated via `xxhash64(natural_keys)` — deterministic and
distributed-safe.  For SCD2, `__valid_from` is included in the hash so each
historical version gets a unique SK.  No configuration is needed; the framework
handles this automatically.

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

| Strategy | Description | When to Use |
| -------- | ----------- | ----------- |
| `cdf` | Change Data Feed (incremental) | Large tables, frequent updates |
| `full` | Full table snapshot | Small dimensions, fact lookups |
| `append` | CDF incremental, drop CDF metadata | Immutable event logs (`append_only: true` only) |

See the `append` section under "append_only" above for details on the
`append` strategy.

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

### streaming (per-source)

Optional sub-config that enables Spark Structured Streaming for a CDF source.
When set, the framework consumes CDF through a streaming query instead of the
default batch `readChangeDataFeed` path.

**Example:**

```yaml
sources:
  - name: silver.customers
    alias: c
    cdc_strategy: cdf
    primary_keys: [customer_id]
    streaming:
      enabled: true
      trigger: available_now          # or: processing_time
      trigger_interval: "30 seconds"  # only used by processing_time
      checkpoint_location: /Volumes/main/etl/_checkpoints/customers
      starting_version: 0
      ignore_deletes: false
      ignore_changes: false
```

**Fields:**

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `enabled` | Yes | `false` | Enable streaming for this source |
| `trigger` | No | `available_now` | `available_now` (one batch, then stop) or `processing_time` (continuous) |
| `trigger_interval` | No | `"30 seconds"` | Interval for `processing_time` trigger |
| `checkpoint_location` | No | Auto-generated | Path for Spark streaming checkpoints |
| `starting_version` | No | Latest | Delta version to start reading CDF from |
| `starting_timestamp` | No | None | Timestamp to start reading CDF from (alternative to version) |
| `ignore_deletes` | No | `false` | Skip CDF delete events |
| `ignore_changes` | No | `false` | Skip CDF update events |

> **`starting_version` vs `starting_timestamp`:** Only one can be set.
> If neither is set, the stream starts from the latest table version
> (only new changes after the stream starts are captured).

**When to use streaming:**

- You need sub-minute freshness (continuous ingestion)
- You want to process changes as they arrive rather than on a schedule
- Your source table has CDF enabled and you're already using `cdc_strategy: cdf`

**When NOT to use streaming:**

- One-off backfills or historical loads (use batch)
- You need zombie batch recovery (not implemented in streaming)
- You need adaptive column pruning (not implemented in streaming)

> **Target table auto-created:** The streaming orchestrator creates the
> target table and seeds default rows on the first micro-batch if it
> doesn't exist. No need to run batch first.
> See [Streaming CDF](STREAMING.md) for full documentation.

### transformation_sql

Spark SQL to transform source data.

**Features:**

- Use source aliases defined in `sources`
- Standard Spark SQL syntax

> [!IMPORTANT]
> **CDF Delete Detection:** If any source uses `cdf` strategy and you need delete detection, you **must** explicitly include `_change_type` in your SELECT clause. The framework will warn if this is missing.

**Example:**

```yaml
# CDF source with delete detection
transformation_sql: |
  SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c._change_type  -- Required for delete detection
  FROM c
```

**Example with JOINs:**

```yaml
# For facts: include _change_type from the primary CDF source
transformation_sql: |
  SELECT
    oi.order_item_id,
    o.order_date,
    c.customer_sk,
    oi.quantity * p.unit_price as amount,
    oi._change_type  -- From primary CDF source (order_items)
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

**Example:**

```yaml
early_arriving_facts:
  - dimension_table: prod_gold.dim_employee
    fact_join_key: employee_id
    dimension_join_key: employee_id
    surrogate_key_col: employee_sk
```

## PII Masking

The framework supports configurable, per-column PII masking declared
directly in the YAML config.  Masking is applied **after**
`transformation_sql` and **before** validation and merge, so sensitive
data never lands in the target Delta table.

### Configuration

```yaml
pii:
  columns:
    - column: email
      strategy: hash              # sha256(value + column_name), irreversible
    - column: address
      strategy: mask              # replace with "**********"
      reveal_prefix: 5            # keep first 5 chars: "12345**********"
      mask_char: "*"              # default
    - column: ssn
      strategy: null              # set to NULL
    - column: phone
      strategy: drop              # remove column entirely
```

### Strategies

| Strategy | What it does | Reversible? | Use case |
|----------|-------------|-------------|----------|
| `hash` | `xxhash64(cast(col as string), col_name)` — 64-bit irreversible hash | No | Pseudonymization. Same input always produces the same hash, so hashed columns can be used for joins and equality checks. Uses xxhash64 (~1.8x faster than SHA-256). |
| `mask` | Replace value with `mask_char * 10`, optionally reveal first N chars | No | Partial visibility (e.g., `"12345**********"` for address). Useful when some characters are needed for analytics (ZIP code, area code). |
| `null` | Set column to `NULL` | No | Column retained for schema compatibility but value removed. |
| `drop` | Remove column from DataFrame entirely | No | Data minimization — downstream consumers never see the column. |

### Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `column` | Yes | — | Column name in the transformed DataFrame |
| `strategy` | No | `mask` | One of `hash`, `mask`, `null`, `drop` |
| `reveal_prefix` | No | `0` | Number of leading characters to keep (only used by `mask`) |
| `mask_char` | No | `"*"` | Character to use for masking (only used by `mask`) |

### Runtime behavior

**Local Spark / OSS Delta:**
- `apply_pii_masking()` runs in the orchestrator after `transformation_sql`
  and before FK fill, validation, and merge.
- PII values are transformed in-flight; the Delta table only ever stores
  masked/hashed values.

**Databricks (Unity Catalog):**
- In addition to transform-time masking, `TableCreator._apply_pii_masks`
  emits `ALTER TABLE ... ALTER COLUMN ... SET MASK (...)` DDL on table
  creation. This enforces role-based read-time masking via Unity Catalog,
  so even users with `SELECT` access see masked values unless they have
  the `MASK` exemption (via `UNMASK` privilege).
- The transform-time masking still runs, so the table stores already-masked
  data. The Delta MASK provides an additional layer for any columns that
  might be added later without PII config.

### Interaction with other features

- **Fingerprint:** PII config is included in the config fingerprint hash,
  so changing PII strategies invalidates the validation-skip cache.
- **SCD2 history:** If a PII column is in `track_history_columns`, the
  hashed/masked value is what gets versioned. Changing the PII strategy
  will produce a new SCD2 version for every row (since the hashdiff changes).
  To avoid this, consider removing PII columns from `track_history_columns`.
- **FK validation:** If a PII column is also a FK column, hashing it will
  break FK validation (the dimension's SK won't match the fact's hashed FK).
  Do not hash FK columns.
- **Schema evolution:** If `drop` strategy is used, the column is removed
  from the DataFrame but the target table schema is not altered. Use
  `schema_evolution: true` if you want the column dropped from the table too.

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

- `dim_customer.yml` - SCD2 dimension
- `dim_product.yml` - SCD1 dimension
- `fact_sales.yml` - Fact with early arriving facts
- `tests/golden/dim_customer.yml` - SCD2 with PII masking (email: hash, address: mask)

## Performance Features (Feature Flags)

The following environment variables control performance optimizations and validation levels:

| Environment Variable                | Default        | Description                                                                                                                                                   |
| ----------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KIMBALL_ENABLE_DEV_CHECKS`         | `0` (Disabled) | **Strict Mode**. Enables expensive data quality counts (validation) and pre-merge grain checks (merger). Recommended for Dev/Test only.                       |
| `KIMBALL_ENABLE_INLINE_OPTIMIZE`    | `0` (Disabled) | Enables `OPTIMIZE` command immediately after every merge. Not recommended for high-frequency or streaming jobs (adds latency). Use async maintenance instead. |
