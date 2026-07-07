# Streaming CDF Support

The streaming module (`kimball.streaming`) adds a Spark Structured Streaming path
to the Kimball framework. It reuses the existing batch merger, watermark manager,
and config loader — only the **read layer** changes.

## When to Use Streaming

| Scenario | Batch | Streaming |
|----------|-------|-----------|
| Low-latency requirements (< 1 minute) | ❌ | ✅ |
| Scheduled hourly/daily runs | ✅ | ❌ |
| Continuous ingestion from Kafka-like sources | ❌ | ✅ |
| One-off historical backfill | ✅ | ❌ |
| Source table has CDF enabled | ✅ | ✅ |
| Need exactly-once processing guarantees | ✅ | ✅ (via checkpoint) |

**Rule of thumb:** start with batch. Only switch to streaming when you need
sub-minute freshness or a continuous ingestion pipeline.

## How It Works

```
Source table (CDF enabled)
       │
       ▼
StreamCdfLoader.stream_cdf()
  └─ spark.readStream.format("delta")
       .option("readChangeFeed", "true")
       .table(source_name)
       │
       ▼
foreachBatch { micro_batch }
  │
  ├─ 1. Drop update_preimage rows
  ├─ 2. Register micro_batch as temp view (source.alias)
  ├─ 3. Run transformation_sql (spark.sql)
  ├─ 4. Merge into target table (reuses batch merger)
  └─ 5. Update etl_control watermark
```

The streaming orchestrator starts one streaming query per CDF source.
Each micro-batch is a regular (non-streaming) DataFrame, so `spark.sql()`
works inside `foreachBatch` — your `transformation_sql` runs identically
to the batch path.

## Configuration

Add a `streaming` block to any CDF source in your YAML:

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
      starting_version: 0            # optional, defaults to latest
      ignore_deletes: false
      ignore_changes: false
```

### Streaming Fields

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
| `per_version` | No | `true` | Process each Delta CDF version in a micro-batch separately; preserves SCD2 history. Set `false` to dedup to latest per micro-batch for throughput. |

> **`starting_version` vs `starting_timestamp`:** Only one can be set.
> If neither is set, the stream starts from the latest table version
> (only new changes after the stream starts are captured).

## Usage

```python
from kimball import StreamingOrchestrator

# Same constructor as Orchestrator
orch = StreamingOrchestrator("configs/dim_customer.yml", spark=spark)
result = orch.run()

# result contains:
#   status: "SUCCESS" | "FAILED"
#   queries: {source_name: StreamingQuery, ...}
#   errors: [...]
#   duration_seconds: float
```

### Fallback to Batch

If no source has `streaming.enabled: true`, `StreamingOrchestrator.run()`
falls back to the regular batch `Orchestrator.run()` automatically.

### Target Table Must Exist

The streaming orchestrator **does not create** the target table. Run the
batch `Orchestrator` once to create it and seed default rows, then switch
to `StreamingOrchestrator` for subsequent runs:

```python
from kimball import Orchestrator, StreamingOrchestrator

# First run: batch creates the target table
Orchestrator("configs/dim_customer.yml", spark=spark).run()

# Subsequent runs: streaming picks up new changes
StreamingOrchestrator("configs/dim_customer.yml", spark=spark).run()
```

## Checkpoints

Spark streaming checkpoints are stored at the `checkpoint_location` path.
If not specified, the framework auto-generates a path:

```
$KIMBALL_STREAMING_CHECKPOINT_ROOT/<sanitised_source_table>
```

Default root: `/tmp/kimball_streaming_checkpoints/`

Set `KIMBALL_STREAMING_CHECKPOINT_ROOT` to a persistent location
(e.g., a Unity Catalog volume or S3 path) for production use.

## Per-version processing

By default (`per_version: true`) the streaming orchestrator processes
**each Delta CDF version inside a micro-batch separately**. This matches
the batch `preserve_all_changes` semantics and guarantees that SCD2
tables record every committed upstream change, even when multiple
versions for the same key arrive in one trigger interval.

With `per_version: false` the orchestrator deduplicates the micro-batch
to the latest change per key and runs a single merge. This is faster but
may collapse multiple upstream changes into one SCD2 interval.

## Limitations

1. **Target table must exist** — streaming does not create tables or seed
   default rows. Run batch once first.

2. **No schema evolution** — the streaming path does not alter the target
   table schema. Use batch for schema changes.

3. **No fingerprint caching** — the streaming path does not compute or
   check config fingerprints. Every micro-batch runs the full merge.

4. **No FK validation** — foreign key checks are not performed in the
   streaming path. Validate FK constraints in a separate batch pipeline.

5. **No adaptive pruning** — column pruning is not applied. All source
   columns are passed through to the merger.

6. **Single source per query** — each CDF source gets its own streaming
   query. Multi-source pipelines start one query per CDF source.

7. **Checkpoint state is not portable** — checkpoint directories are tied
   to the Spark application. Moving or copying them may cause corruption.

8. **`available_now` trigger processes all available data then stops** —
   this is the recommended trigger for most use cases. Use
   `processing_time` only when you need continuous ingestion.

## Running Tests

```bash
# All unit tests (no Spark needed for streaming tests)
docker-compose run --rm kimball-tests python -m pytest tests/unit/streaming/ -v

# Streaming integration tests (requires Java + Delta)
docker-compose run --rm kimball-tests python -m pytest tests/integration/test_streaming_cdf.py -v

# Full test suite
docker-compose run --rm kimball-tests python -m pytest tests/ -v
```
