# Performance Optimization Guide

This document catalogs the algorithmic complexity of core operations and provides guidance on performance tuning.

For reproducible execution, baseline compatibility, result retention, and
regression policy, see [Benchmarking](BENCHMARKING.md).

## Execution model and action budget

The framework builds Spark plans lazily. Reads, projections, joins, windows,
aggregations, and temporary views do not execute until an action such as
collect, count, isEmpty, a Delta MERGE, or a write.

The batch orchestrator first builds a source work plan from one control-table
read and Delta source versions. A caught-up run performs no target, batch-start,
or completion write. Incremental ranges that produce zero output still advance
their watermark after successful processing; an empty full snapshot is instead
passed to SCD2 reconciliation so it can expire missing current rows.

The framework intentionally retains exact natural-key and merge-grain
validation. When they use the same key tuple, the merge reuses the natural-key
result instead of repeating the grouped shuffle. Approximate uniqueness remains
an observability option, not a merge-safety replacement.

SCD1 uses one conditional Delta merge. The matched-update predicate compares
tracked columns with null-safe equality, so unchanged rows are not rewritten and
no source/target hash preflight is required. SCD4 persists its narrow source
lineage only across its required current-table and history-table writes.

Contract findings are appended in one Delta write per validation stage. Streaming
micro-batches are persisted and exposed as a temporary view; the framework no
longer writes and drops a temporary Delta table for every batch. Per-version
streaming still enumerates versions because preserving intermediate SCD2 history
is a semantic requirement.

## Beta control-table reset

etl_control is now created without static target/source partitioning and no
longer performs automatic schema migration. Before adopting this beta version,
drop the existing control and observability state tables and run a deliberate
full reload. Business target tables are not dropped by this change.

## Design Philosophy (Knuth-style)

Performance optimization in this framework follows these principles:

1. **Correctness first**: Never sacrifice data integrity for speed
2. **Algorithmic efficiency**: Choose O(n) over O(n²), avoid unnecessary shuffles
3. **Minimize actions**: Reduce Spark job triggers (`.count()`, `.collect()`, etc.)
4. **Bounded operations**: Prefer `limit(1).isEmpty()` over `count() == 0`
5. **Tiered validation**: Expensive checks should be opt-in via dev mode

## Performance Modes

The framework supports two operational modes controlled by environment variable:

```bash
# Production mode (default) - Fast-path validation
# Skips expensive counts, uses existence checks
export KIMBALL_ENABLE_DEV_CHECKS=0

# Development mode - Full validation with detailed metrics
# Performs full counts, collects samples, detailed logging
export KIMBALL_ENABLE_DEV_CHECKS=1
```

### Production Mode Optimizations

- **Validation**: `limit(1).isEmpty()` instead of `count()`
- **Total row counts**: Skipped (set to -1 in metrics)
- **Duplicate detection**: Early exit on first duplicate found
- **Sample collection**: Minimal (5 rows max)

### Development Mode Trade-offs

- **Full counts**: Every validation computes exact row counts
- **Detailed samples**: Collects up to specified sample size
- **Comprehensive metrics**: All metrics populated for debugging
- **Cost**: 2-5x slower on large datasets due to additional actions

## Algorithmic Complexity by Operation

### Data Loading (`loader.py`)

#### CDF Deduplication
```python
window = Window.partitionBy(keys).orderBy(version.desc(), change_type_priority)
df = df.withColumn("_rn", row_number().over(window)).filter("_rn == 1")
```

**Complexity**: 
- Shuffle: O(n) to partition by keys
- Sort: O(k log k) per partition where k = rows per key
- Overall: O(n log k) where n = total rows, k = avg rows per key

**When it matters**: Large incremental batches with many versions per key

**Optimization opportunities**:
- Ensure columns are projected BEFORE window (minimize shuffle data)
- Consider `preserve_all_changes=true` config to skip dedup entirely if needed
- Alternative: `groupBy(keys).agg(max(struct(...)))` but harder to get priority logic correct

### Data Validation (`validation.py`)

#### Uniqueness Check
```python
duplicates = df.groupBy(*columns).count().filter(col("count") > 1)
```

**Complexity**: O(n) shuffle + O(n) aggregation

**When it matters**: Always in dev mode; production mode uses fast exit

**Cost in production**: 
- Best case (no duplicates): One shuffle + limit(1)
- Worst case (many duplicates): Full shuffle + count

#### Referential Integrity
```python
orphans = fact_df.join(dim_df.select(key), fk_col == key, "left_anti")
```

**Complexity**: O(n + m) broadcast join OR O(n log n + m log m) sort-merge join

**When it matters**: Large fact tables with high FK cardinality

**Optimization**: 
- Dimension keys should be broadcastable (< 10MB compressed)
- Use `spark.sql.autoBroadcastJoinThreshold` to control

### Merging (`merger.py`)

#### Duplicate Source Validation
```python
duplicates = source_df.groupBy(*join_keys).count().filter(col("count") > 1)
```

**Complexity**: O(n) shuffle for entire source batch

**When it matters**: Always for SCD2 dimensions (correctness-critical)

**Cost analysis**:
- SCD2: Always enabled (prevents merge errors)
- Future optimization: Make conditional on CDC strategy
  - CDF with upstream dedup: Can skip
  - Full snapshot: Always check

#### SCD2 Change Detection
```python
target_df = delta_table.toDF().filter("__is_current = true").join(source_keys, keys, "semi")
```

**Complexity**: O(n) for source, O(m) for target filter, O(min(n,m)) for semi-join

**When it matters**: Large dimensions with many current rows

**Optimization**:
- Delta's data skipping helps if join keys are in Z-order/cluster columns
- Semi-join is more efficient than inner join (doesn't duplicate rows)

### Brokered Type 7 Lookup and Inferred Members

#### Missing Key Detection
```python
candidates = fact_df.groupBy(*natural_keys).agg(min(event_time))
missing = candidates.join(dimension_intervals, range_condition, "left_anti")
```

**Complexity**: 
- One source grouping: O(n) with a shuffle by natural key
- One interval anti-join before the inferred-member MERGE
- One point-in-time lookup that stamps both the row and durable keys

**When it matters**: Large fact batches with high dimension cardinality

Identity maps are validated once per `KeyBroker` instance and reused across
relationships. Exact `error` and `skeleton` barriers use an eager, bounded
existence action because allowing an unresolved fact to mutate its target
would violate the key contract. The benchmark suite measures the complete
range lookup rather than timing only lazy DataFrame construction.

## Spark Action Audit

Actions that trigger Spark jobs (ranked by cost):

### High Cost (Full Data Scan + Shuffle)
- `df.count()` - Full table scan
- `df.groupBy(...).count()` - Shuffle + aggregation
- `df.distinct().count()` - Shuffle + dedup + count

### Medium Cost (Partial Scan)
- `df.filter(...).count()` - Full scan with predicate
- `df.join(..., "left_anti")` - Join execution

### Low Cost (Bounded)
- `df.limit(1).isEmpty()` - Stops at first row
- `df.limit(n).collect()` - Bounded collection
- `delta_table.history().limit(1)` - Metadata only

## Configuration Tuning

### Validation Strategy

```yaml
# config.yml
validation:
  mode: production  # or: development, strict
  
  # Strict mode: Production with selective full checks
  strict_checks:
    - unique: [customer_id]  # Force full uniqueness check
    - referential_integrity: all  # Check all FKs exhaustively
```

### Merge Optimization

```yaml
# For large SCD2 dimensions
scd_type: 2
optimize_after_merge: true  # Run OPTIMIZE after merge
cluster_by: [customer_id]   # Liquid clustering for join performance

# For high-throughput facts
merge_keys: [order_id]      # Explicit merge keys for better pruning
```

### CDF Loading

```python
# Always specify ending_version for bounded reads
loader.load_cdf(
    table_name="silver.orders",
    starting_version=100,
    ending_version=105,  # Prevents race conditions
    deduplicate_keys=["order_id"]
)
```

## Profiling Commands

### Measure validation overhead
```python
import time

# Production mode
os.environ["KIMBALL_ENABLE_DEV_CHECKS"] = "0"
start = time.time()
validator.run_config_tests(config, df)
prod_time = time.time() - start

# Dev mode
os.environ["KIMBALL_ENABLE_DEV_CHECKS"] = "1"
start = time.time()
validator.run_config_tests(config, df)
dev_time = time.time() - start

print(f"Dev mode overhead: {dev_time / prod_time:.2f}x")
```

### Analyze merge metrics
```python
metrics = merger.merge(source_df)
print(f"Rows processed: {metrics.num_source_rows}")
print(f"Rows inserted: {metrics.num_target_rows_inserted}")
print(f"Rows updated: {metrics.num_target_rows_updated}")
```

## When to Optimize

Follow this decision tree:

1. **Is correctness at risk?** → Fix correctness first, optimize never
2. **Is runtime > 10 minutes?** → Profile and identify bottleneck
3. **Is validation the bottleneck?** → Enable production mode
4. **Is deduplication the bottleneck?** → Review source data quality
5. **Is merge the bottleneck?** → Check clustering/Z-order on join keys
6. **Is skeleton generation the bottleneck?** → Review dimension arrival patterns

## Anti-Patterns to Avoid

❌ **Never** call `.count()` in a loop
❌ **Never** collect large datasets to driver (use `limit()`)
❌ **Never** perform validation in production without fast-path mode
❌ **Never** run duplicate checks after upstream dedup (redundant work)
❌ **Never** use global sorts unless absolutely necessary (sequence keys)

✅ **Always** use `limit(1).isEmpty()` for existence checks
✅ **Always** project columns before shuffles
✅ **Always** use semi-joins instead of inner joins for filtering
✅ **Always** leverage Delta's data skipping with clustering

## Future Optimizations

These are documented but not yet implemented:

1. **Validation caching**: Cache DF once, run multiple validations
2. **Probabilistic checks**: Sample-based duplicate detection
3. **Adaptive validation**: Auto-detect when full checks are needed
4. **Merge bypass**: Skip duplicate check when CDC guarantees uniqueness
5. **Aggregation-based dedup**: Replace `row_number()` with `max(struct())`

## References

- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [Delta Lake Best Practices](https://docs.delta.io/latest/best-practices.html)
- Knuth, D. (1974). "Structured Programming with go to Statements"
- The Data Warehouse Toolkit (Kimball & Ross)
