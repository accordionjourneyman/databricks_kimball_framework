# Architecture Overview

## Design Principles

1. **Declarative Configuration** - Define what, not how
2. **Separation of Concerns** - Business logic (SQL) separate from infrastructure (CDF, watermarking)
3. **Exactly-Once Semantics** - Watermark-based processing guarantees
4. **Kimball Compliance** - Follows dimensional modeling best practices
5. **Delta Lake Native** - Leverages ACID transactions and time travel

## Component Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    YAML Configuration                     │
│  (Declarative table definitions, transformations, keys)  │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────┐
│                     Orchestrator                          │
│  • Coordinates pipeline execution                        │
│  • Manages component lifecycle                           │
│  • Handles error recovery                                │
└──┬────────┬────────┬────────┬────────┬────────┬─────────┘
   │        │        │        │        │        │
   ▼        ▼        ▼        ▼        ▼        ▼
┌─────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌────────┐ ┌──────────┐
│ ETL ││ Data │ │Trans-│ │Merge │ │Skeleton│ │Resilience│
│Ctrl ││Loader│ │former│ │  r   │ │  Gen   │ │ Module   │
└─────┘ └──────┘ └──────┘ └──────┘ └────────┘ └──────────┘
   │        │        │        │        │           │
   ▼        ▼        ▼        ▼        ▼           ▼
┌──────────────────────────────────────────────────────────┐
│            Delta Lake Tables                              │
│  • Source (Silver)                                       │
│  • Target (Gold)                                         │
│  • ETL Control (Audit)                                   │
│  • Checkpoints (optional)                                │
│  • Staging Registry (optional)                           │
└──────────────────────────────────────────────────────────┘
```

## Core Components

### ConfigLoader

**Responsibility:** Parse and validate YAML configurations

**Key Methods:**

- `load_config(path)` - Load and parse YAML file
- Validates required fields
- Supports Jinja2 templating

### ETLControlManager

**Responsibility:** Kimball-style ETL auditing with watermarks and batch lifecycle tracking

**Configuration:**

```python
import os
os.environ["KIMBALL_ETL_SCHEMA"] = "gold"  # Set once at notebook start
```

**Key Methods:**

- `get_watermark(target, source)` - Retrieve last processed version
- `batch_start(target, source)` - Mark batch as RUNNING
- `batch_complete(target, source, version, rows_read, rows_written)` - Mark SUCCESS
- `batch_fail(target, source, error_message)` - Mark FAILED

**Table Schema:**

```sql
CREATE TABLE etl_control (
  target_table STRING,
  source_table STRING,
  last_processed_version LONG,
  batch_id STRING,
  batch_started_at TIMESTAMP,
  batch_completed_at TIMESTAMP,
  batch_status STRING,  -- RUNNING, SUCCESS, FAILED
  rows_read LONG,
  rows_written LONG,
  error_message STRING,
  updated_at TIMESTAMP
)
PARTITIONED BY (target_table, source_table)
```

### DataLoader

**Responsibility:** Read source data using appropriate strategy

**Key Methods:**

- `load_full_snapshot(table)` - Full table read
- `load_cdf(table, start_version, deduplicate_keys)` - Incremental CDF read with deduplication
- `get_latest_version(table)` - Get current table version

**CDF Deduplication:**
When a single CDF batch contains multiple operations on the same row (e.g., insert then update), `deduplicate_keys` ensures only the final state is processed:

```python
# Config specifies primary_keys for deduplication
sources:
  - name: silver.customers
    cdc_strategy: cdf
    primary_keys: [customer_id]  # Used for deduplication
```

**CDF Metadata:**

- `_change_type` - insert, update_preimage, update_postimage, delete
- `_commit_version` - Delta version number
- `_commit_timestamp` - Change timestamp

### DeltaMerger

**Responsibility:** Execute MERGE operations with SCD logic

**Key Methods:**

- `merge()` - Main entry point
- `_merge_scd1()` - Type 1 merge (overwrite)
- `_merge_scd2()` - Type 2 merge (versioning)

**SCD2 Algorithm:**

1. Compute hashdiff on source
2. Join with current target rows
3. Identify changes (hashdiff mismatch)
4. Duplicate changed rows (UPDATE + INSERT)
5. Generate surrogate keys for INSERTs
6. Execute atomic MERGE

### SkeletonGenerator

**Responsibility:** Handle early arriving facts

**Key Methods:**

- `generate_skeletons()` - Create placeholder dimension rows

**Algorithm:**

1. Identify distinct keys in fact data
2. LEFT ANTI JOIN with dimension
3. Generate skeleton rows for missing keys
4. Append to dimension table

### KeyGenerator (Abstract)

**Responsibility:** Generate surrogate keys

**Implementations:**

- `IdentityKeyGenerator` - Uses Delta Identity Columns
- `HashKeyGenerator` - xxhash64 of natural keys
- `SequenceKeyGenerator` - Row number + max key

### Resilience Module (kimball.resilience)

**Responsibility:** Optional production features for crash resilience and observability

**Components:**

- `QueryMetricsCollector` - Collect pipeline execution metrics
- `PipelineCheckpoint` - Save/restore pipeline state for DAG resumability
- `StagingCleanupManager` - Cleanup orphaned staging tables with ACID registry
- `StagingTableManager` - Context manager for staging table lifecycle

**Configuration (opt-in):**

```python
import os
# Enable all features
os.environ["KIMBALL_MODE"] = "full"

# Or enable specific features
os.environ["KIMBALL_ENABLE_CHECKPOINTS"] = "1"
os.environ["KIMBALL_ENABLE_STAGING_CLEANUP"] = "1"
os.environ["KIMBALL_ENABLE_METRICS"] = "1"
```

## Data Flow

### Initial Load (No Watermark)

```
1. ConfigLoader reads YAML
2. ETLControlManager.batch_start() marks RUNNING
3. ETLControlManager.get_watermark() returns None (first run)
4. DataLoader performs full snapshot
5. Transformation SQL executes
6. DeltaMerger inserts all rows
7. ETLControlManager.batch_complete() marks SUCCESS + records version
```

### Incremental Load (With Watermark)

```
1. ConfigLoader reads YAML
2. ETLControlManager.batch_start() marks RUNNING
3. ETLControlManager.get_watermark() returns last version (e.g., V5)
4. DataLoader reads CDF from V6+ (with deduplication via primary_keys)
5. SkeletonGenerator checks for missing dimension keys
6. Transformation SQL executes (CDF + snapshots)
7. DeltaMerger:
   - SCD1: UPDATE existing, INSERT new
   - SCD2: EXPIRE changed, INSERT new versions
8. ETLControlManager.batch_complete() marks SUCCESS + records new version
```

## Key Design Patterns

### Temp View Aliasing

Sources are registered as Spark temp views, allowing clean SQL:

```python
# Framework code
for source in config.sources:
    df = loader.load(source)
    df.createOrReplaceTempView(source.alias)

# User SQL
transformation_sql = """
  SELECT oi.*, c.customer_sk
  FROM oi  -- Alias for order_items
  JOIN c   -- Alias for dim_customer
"""
```

### Union Approach (SCD2)

Single atomic MERGE handles both UPDATE and INSERT:

```python
# Duplicate changed rows
rows_to_expire = changed.withColumn("__action", lit("UPDATE"))
rows_to_insert = changed.withColumn("__action", lit("INSERT"))
staged = rows_to_expire.union(rows_to_insert)

# Single MERGE
MERGE INTO target
USING staged
ON target.key = staged.key AND target.__is_current
WHEN MATCHED AND staged.__action = 'UPDATE'
  THEN UPDATE SET __is_current = false, __valid_to = now()
WHEN NOT MATCHED
  THEN INSERT ...
```

### Hashdiff Change Detection

Deterministic hash of tracked columns:

```python
hashdiff = xxhash64(
  coalesce(col("first_name"), ""),
  coalesce(col("last_name"), ""),
  coalesce(col("email"), "")
)
```

## Error Handling

### Idempotency

Re-running a batch is safe:

- Watermark not updated until success
- SCD2 hashdiff prevents duplicate versions
- MERGE is atomic (all or nothing)

### Failure Scenarios

| Failure Point                      | Recovery                                 | ETL Control Status |
| ---------------------------------- | ---------------------------------------- | ------------------ |
| During source read                 | No data written, retry safe              | FAILED             |
| During transformation              | No data written, retry safe              | FAILED             |
| During MERGE                       | Delta ACID rollback, retry safe          | FAILED             |
| After MERGE, before batch_complete | Data written, re-run is no-op (hashdiff) | RUNNING (stale)    |

## Performance Considerations

### CDF vs Snapshot

- **CDF:** Reads only delta log (KB) + changed data
- **Snapshot:** Reads entire table (GB/TB)
- **Recommendation:** Use CDF for tables > 1M rows or > 1GB

### Partitioning

Not currently implemented, but recommended for:

- Fact tables by date
- Large dimensions by region/category

### Z-Ordering

Not currently implemented, but recommended for:

- Natural keys (for MERGE performance)
- Common filter columns

## Future Enhancements

- [x] ~~Automatic partitioning based on config~~ (Liquid Clustering)
- [x] ~~Metrics and monitoring~~ (ETL Control Table with row counts)
- [x] ~~Parallel execution~~ (PipelineExecutor, Databricks Jobs for_each)
- [ ] Z-ordering optimization
- [ ] Error bucket for quarantine
- [ ] Late arriving dimension handling
- [ ] Factless fact tables
- [ ] Bridge tables for many-to-many
