# Configuration Guide

Complete reference for YAML configuration files.

## Table Configuration Structure

```yaml
# Required: Target table name
table_name: {{ env }}_gold.dim_customer

# Required: Table type
table_type: dimension  # or: fact

# Required for dimensions: SCD type
scd_type: 7  # 1, 2, 4, 6, or 7

# Required: Key definitions
keys:
  surrogate_key: customer_sk
  durable_key: customer_dk  # required only for SCD7
  natural_keys: [customer_id]  # Can be composite: [region, customer_id]

# Type 7 generates a durable key from the canonical natural-key payload and a
# row surrogate key from that payload plus effective_at. SHA-256 fingerprints
# provide collision evidence for both compact BIGINT keys.

# Required for SCD2/SCD7: columns that trigger new versions
track_history_columns:
  - first_name
  - last_name
  - email
  - address

# Four default members are always seeded: -1 missing, -2 not applicable,
# -3 not yet available, and -4 bad value.

# Optional: Enable automatic schema evolution (default: false)
schema_evolution: true

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
- `4` - Current dimension plus separate attribute history
- `6` - Type 2 rows with overwritten current-value attributes
- `7` - Type 2 history with durable and version keys for as-is/as-was facts

> [!NOTE]
> **SCD2 Multi-Version Batches**: When multiple changes for the same natural key
> occur within a single batch, the framework preserves ALL versions using a
> canonical single-pass SCD2 merge stages all source versions and preserves their validity chain
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

### Surrogate and durable keys

Dimensions use canonical, type-normalized payloads and deterministic
`xxhash64`. SCD2 version keys include `effective_at`. SCD7 adds a durable
key from only the natural key and stores SHA-256 fingerprints for both
payloads. Before an SCD7 merge, a collision gate compares incoming and
stored fingerprints and rejects collisions or generated values in the
reserved `-1..-4` namespace.

See [Type 7, key brokering, identity maps, and null semantics](SCD7_KEYS_AND_NULLS.md)
for the complete contract and fact-side YAML.

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

`effective_at` is required for SCD2 and SCD7. It defines deterministic business-time
`__valid_from`/`__valid_to` boundaries and makes replays and out-of-order
backfills testable. Processing time is not an implicit fallback.

```yaml
effective_at: updated_at
```

The transformation must project this column. If no reliable business timestamp
exists, materialize an explicit source-side ingestion timestamp and document
that its semantics are processing time.

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

### Brokered foreign keys and early arrivals

Early-arriving behavior belongs to each `foreign_keys[].lookup` declaration.
The accepted policies are `skeleton`, `default`, and `error`; the removed
top-level `early_arriving_facts`, `early_arriving_dimensions`, and
`identity_bridge` blocks fail strict configuration loading. See
[the normative key guide](SCD7_KEYS_AND_NULLS.md) for complete examples,
reserved-member precedence, identity-map schema, and replay evidence.

## PII transformation and tokenization

PII policy is applied after transformation SQL and before validation/merge. The
policy must distinguish cryptographic pseudonymization from fast analytical
encoding.

```yaml
pii:
  columns:
    - column: email
      strategy: tokenize
      secret_ref: databricks://security/pii_hmac
    - column: search_key
      strategy: fast_hash
    - column: address
      strategy: mask
      reveal_prefix: 5
      mask_char: "*"
    - column: ssn
      strategy: null
    - column: free_text
      strategy: drop
```

| Strategy | Guarantee | Intended use |
| --- | --- | --- |
| `tokenize` | Deterministic keyed HMAC-SHA-256, null preserving. Requires `secret_ref`. | Security pseudonymization and equality joins where every producer uses the same controlled key. |
| `fast_hash` | Unsalted `xxhash64`; deterministic but vulnerable to dictionary attacks. | Non-sensitive equality encoding only. |
| `hash` | Deprecated alias for `fast_hash`; emits a warning. | Migration only. |
| `mask` | Produces a string mask and optional visible prefix. | Limited display; numeric inputs are not cast back to a numeric type. |
| `null` | Replaces values with typed null. | Schema-preserving data minimization. |
| `drop` | Removes the column from the transformed DataFrame. | Strongest pipeline-level minimization. |

Secrets use explicit references:

- `databricks://scope/key` resolves through Databricks Secrets at runtime.
- `env://NAME` resolves from a controlled local/CI environment.

The manifest stores only the reference. Secret values are never included in
configuration fingerprints, plans, or exception messages. Do not tokenize
foreign keys unless both sides are deliberately tokenized with the same key.
Changing a tokenization key or PII strategy changes stored values and can create
new SCD versions; treat it as a migration/backfill.

Delta/Unity Catalog read-time masks are separate from transform-time storage
protection. The framework may apply supported read-time mask DDL when creating
tables, but grants, exemptions, and policy ownership remain governance-plane
responsibilities.

## Standard Columns

The framework automatically manages these columns:

### SCD2/SCD7 Columns

- `__valid_from` - Row effective timestamp
- `__valid_to` - Exclusive row expiration timestamp (`9999-12-31...` = current)
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

- `dim_customer.yml` - SCD7 dual-key dimension
- `dim_product.yml` - SCD1 dimension
- `fact_sales.yml` - Fact with brokered Type 7 customer keys
- `tests/golden/dim_customer.yml` - SCD7 regression fixture

## Performance Features (Feature Flags)

The following environment variables control performance optimizations and validation levels:

| Environment Variable                | Default        | Description                                                                                                                                                   |
| ----------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KIMBALL_ENABLE_DEV_CHECKS`         | `0` (Disabled) | **Strict Mode**. Enables expensive data quality counts (validation) and pre-merge grain checks (merger). Recommended for Dev/Test only.                       |
| `KIMBALL_ENABLE_INLINE_OPTIMIZE`    | `0` (Disabled) | Enables `OPTIMIZE` command immediately after every merge. Not recommended for high-frequency or streaming jobs (adds latency). Use async maintenance instead. |

## Kimball Modeling Metadata

The following fields make a fact model self-describing. They are additive and backward-compatible: existing fact YAML remains valid until a `fact_pattern` is declared.

### Fact patterns, grain, and measures

```yaml
table_type: fact
merge_keys: [snapshot_date, product_id, warehouse_id]
grain: one product per warehouse per snapshot day
fact_pattern: periodic_snapshot
snapshot_period: day
measures:
  - name: on_hand_quantity
    aggregation: sum
    additivity: semi_additive
    non_additive_dimensions: [snapshot_date]
```

`fact_pattern` is `transaction`, `periodic_snapshot`, or `accumulating_snapshot`. When it is supplied, `grain` is required. Periodic snapshots also require `snapshot_period` (`day`, `week`, `month`, `quarter`, or `year`). A semi-additive measure must declare the dimensions across which it cannot be summed.

Accumulating snapshots declare ordered lifecycle milestones:

```yaml
fact_pattern: accumulating_snapshot
grain: one order lifecycle
milestones:
  - name: ordered
    column: ordered_at
    order: 1
  - name: shipped
    column: shipped_at
    order: 2
  - name: delivered
    column: delivered_at
    order: 3
```

At least two milestones are required and `order` values must be unique. The metadata documents and validates the model shape; lifecycle update SQL remains the pipeline transformation responsibility.

### Role-playing, degenerate, and junk dimensions

A role-playing dimension uses explicit FK metadata, never a column-name heuristic:

```yaml
foreign_keys:
  - column: order_date_sk
    references: demo_gold.dim_date
    dimension_key: date_sk
    role_playing: true
    role: order_date
  - column: ship_date_sk
    references: demo_gold.dim_date
    dimension_key: date_sk
    role_playing: true
    role: ship_date
```

Roles must be unique within a fact. The enterprise bus matrix shows one `dim_date` column with role annotations such as `X (order_date, ship_date)`.

A degenerate dimension is a business identifier intentionally retained on the fact:

```yaml
degenerate_dimensions: [order_number, invoice_number]
```

The framework validates that every declared degenerate column exists in the
transformed fact output. It does not create a separate table or alter merge
behavior.

A managed junk dimension groups low-cardinality flags into a deterministic key:

```yaml
junk_dimensions:
  - dimension_table: demo_gold.dim_order_flags
    surrogate_key: order_flags_sk
    source_columns: [is_gift, is_priority, is_first_order]
```

Before the fact merge, the framework idempotently merges new distinct
combinations into the junk table, detects deterministic-key collisions, adds
`order_flags_sk` to the fact DataFrame, and validates it as an FK. Junk keys
must be unique in one fact config and cannot also be degenerate dimensions.

### Table and column descriptions

Use YAML to manage catalog documentation:

```yaml
table_description: Customer master used by sales and service facts.
column_descriptions:
  customer_sk: Framework-generated surrogate key.
  customer_id: Source-system business identifier.
  email: Customer email address; PII.
```

Descriptions are synchronized after a successful data merge, after schema
evolution has made new columns available. The framework validates referenced
columns, compares the desired state with the normalized manifest in table
property `kimball.descriptions.manifest`, and emits DDL only for changes.
Removing a previously YAML-owned description clears that comment with `NULL`;
unrelated comments are not claimed. Description text is SQL-escaped.

A description-sync failure is best-effort: data remains loaded and the next run retries the metadata synchronization. Empty descriptions are rejected during configuration loading.

## Project dependencies and planning

Production projects declare every in-project upstream explicitly:

```yaml
table_name: gold.fact_sales
depends_on: [gold.dim_customer, gold.dim_product]
```

References in sources and foreign keys are used to infer the effective graph.
Production compilation rejects an inferred dependency omitted from
`depends_on`, cycles, missing declared upstreams, duplicate target writers, and
auxiliary-table writer conflicts. See [Production Readiness](PRODUCTION_READINESS.md).

## Supplier contracts and validation budgets

Use an exact ODCS pin for producer/consumer CI:

```yaml
sources:
  - name: prod_silver.customers
    alias: customers
    contract_ref: ../../contracts/customer-contract/1.0.0.odcs.yaml
```

Inline contracts can additionally declare `schema`, `cdc`, `freshness`,
`quality`, `temporal`, and an execution budget:

```yaml
contract:
  id: customer-events
  version: 1.2.0
  schema:
    customer_id: {type: bigint, nullable: false}
    event_at: {type: timestamp, nullable: false}
  cdc:
    required: true
    primary_key: [customer_id]
  temporal:
    event_time_column: event_at
    allowed_lateness: 2 hours
    late_event_severity: warn
    out_of_order_severity: error
  validation:
    mode: sampled
    sample_fraction: 0.1
    sample_seed: 17
    max_sample_rows: 1000000
    max_failure_samples: 5
    max_actions: 8
```

`mode` is `full`, `sampled`, or `approximate`. Approximate mode uses
approximate distinct counting for uniqueness. Sampling is explicit rather than
silently changing the meaning of a full check. See [Data Supplier Contracts](DATA_CONTRACTS.md).

## Observability storage

```yaml
observability:
  enabled: true
  event_table: etl_data_quality_events
  temporal_state_table: etl_contract_temporal_state
  write_failure: warn
  webhook_env: KIMBALL_ALERT_WEBHOOK_URL
  alert_on: [error]
```

`write_failure: warn` preserves the transformation result when optional event
or temporal-state storage fails and logs the exception type. `error` fails the
run. Error-severity validation findings always block regardless of this storage
policy. Batch and streaming commit temporal maxima only after their merge and
watermark succeed.
