# Parallel ETL Execution Guide

This guide covers how to run multiple Kimball pipelines in parallel while respecting dimensional modeling dependencies.

## Table of Contents

1. [Overview](#overview)
2. [Databricks Jobs (Production)](#databricks-jobs-production)
3. [PipelineExecutor (Notebooks)](#pipelineexecutor-notebooks)
4. [ETL Control Table](#etl-control-table)
5. [Error Handling & Retry](#error-handling--retry)
6. [Best Practices](#best-practices)

---

## Overview

### The Kimball Dependency Rule

In dimensional modeling, **dimensions must be loaded before facts**. This is because fact tables contain foreign keys (surrogate keys) that reference dimension tables.

```
Wave 1: Independent Dimensions (parallel)
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ dim_customer │  │ dim_product  │  │  dim_date    │
└──────────────┘  └──────────────┘  └──────────────┘
        │                │                 │
        └────────────────┼─────────────────┘
                         │
                         ▼
Wave 2: Facts (parallel, after dimensions complete)
        ┌──────────────┐  ┌──────────────┐
        │  fact_sales  │  │fact_inventory│
        └──────────────┘  └──────────────┘
```

### Two Execution Options

| Approach | Use Case | Parallelism | Monitoring |
|----------|----------|-------------|------------|
| **Databricks Jobs** | Production | Native task parallelism | Databricks UI, alerting |
| **PipelineExecutor** | Notebooks, dev | ThreadPoolExecutor | Print statements |

---

## Databricks Jobs (Production)

**This is the RECOMMENDED approach for production workloads.**

### Single Parameterized Notebook

Create a notebook that accepts a config file as a parameter:

```python
# /pipelines/run_kimball_pipeline.py

# Configure ETL schema (where to store ETL control table)
import os
os.environ["KIMBALL_ETL_SCHEMA"] = "gold"

# Widget parameter determines which config to run
dbutils.widgets.text("config_file", "")
config_file = dbutils.widgets.get("config_file")

from kimball import Orchestrator

result = Orchestrator(config_file).run()
print(f"Completed: {result['rows_written']} rows written")
```

### Job Definition with `for_each`

Create a Databricks Job that uses `for_each` tasks for parallel execution:

```yaml
# Job: kimball_daily_load
name: "Kimball Daily Load"

tasks:
  # Wave 1: All dimensions in parallel
  - task_key: load_dimensions
    for_each_task:
      inputs: 
        - "configs/dim_customer.yml"
        - "configs/dim_product.yml"
        - "configs/dim_date.yml"
        - "configs/dim_store.yml"
      concurrency: 4  # Run up to 4 in parallel
      task:
        notebook_task:
          notebook_path: /pipelines/run_kimball_pipeline
          base_parameters:
            config_file: "{{input}}"

  # Wave 2: All facts in parallel (after dimensions complete)
  - task_key: load_facts
    depends_on:
      - task_key: load_dimensions
    for_each_task:
      inputs:
        - "configs/fact_sales.yml"
        - "configs/fact_inventory.yml"
      concurrency: 2
      task:
        notebook_task:
          notebook_path: /pipelines/run_kimball_pipeline
          base_parameters:
            config_file: "{{input}}"

# Job cluster configuration
job_clusters:
  - job_cluster_key: etl_cluster
    new_cluster:
      spark_version: "14.3.x-scala2.12"
      node_type_id: "Standard_DS3_v2"
      num_workers: 2
```

### Benefits of Databricks Jobs

✅ Native Databricks monitoring, alerting, and retry handling  
✅ Automatic parallelization within each wave  
✅ Single notebook, parameterized via widgets  
✅ Visual DAG in Databricks UI  
✅ Built-in retry with exponential backoff  
✅ Designed for Databricks (that's the point of this framework)  

---

## PipelineExecutor (Notebooks)

For demo notebooks and ad-hoc testing, use the `PipelineExecutor`:

```python
import os
from kimball import PipelineExecutor

# Configure ETL schema once
os.environ["KIMBALL_ETL_SCHEMA"] = "gold"

# Create executor with config paths
executor = PipelineExecutor(
    config_paths=[
        "configs/dim_customer.yml",
        "configs/dim_product.yml",
        "configs/dim_date.yml",
        "configs/fact_sales.yml",
        "configs/fact_inventory.yml",
    ],
    max_workers=4,        # Parallel pipelines per wave
    stop_on_failure=True  # Stop if any pipeline fails
)

# Preview execution plan (no execution)
executor.dry_run()

# Run all pipelines
summary = executor.run()
print(summary)
```

### Example Output

```
############################################################
# Kimball Pipeline Executor
# Dimensions: 3, Facts: 2
# Max Workers: 4
############################################################

============================================================
Wave: Dimensions (3 pipelines)
============================================================
  Starting: gold.dim_customer
  Starting: gold.dim_product
  Starting: gold.dim_date
  ✓ Completed: gold.dim_date (12.3s, 365 rows)
  ✓ Completed: gold.dim_product (18.5s, 1200 rows)
  ✓ Completed: gold.dim_customer (24.1s, 50000 rows)

============================================================
Wave: Facts (2 pipelines)
============================================================
  Starting: gold.fact_sales
  Starting: gold.fact_inventory
  ✓ Completed: gold.fact_inventory (45.2s, 120000 rows)
  ✓ Completed: gold.fact_sales (62.8s, 500000 rows)

============================================================
Execution Summary:
  Total Pipelines: 5
  Successful: 5
  Failed: 0
  Skipped: 0
  Total Rows Read: 671,565
  Total Rows Written: 671,565
  Total Duration: 87.1s
============================================================
```

### Accessing Results

```python
# Get detailed results
for result in summary.results:
    print(f"{result.table_name}: {result.status} ({result.rows_written} rows)")

# Check for failures
failed = [r for r in summary.results if r.status == "FAILED"]
if failed:
    for f in failed:
        print(f"FAILED: {f.table_name} - {f.error_message}")
```

---

## ETL Control Table

The framework automatically tracks batch execution in the `etl_control` table:

```sql
-- Table schema
CREATE TABLE gold.etl_control (
    target_table STRING NOT NULL,
    source_table STRING NOT NULL,
    last_processed_version LONG,
    batch_id STRING,
    batch_started_at TIMESTAMP,
    batch_completed_at TIMESTAMP,
    batch_status STRING,        -- 'RUNNING', 'SUCCESS', 'FAILED'
    rows_read LONG,
    rows_written LONG,
    error_message STRING,
    updated_at TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (target_table, source_table)
```

### Operational Queries

```sql
-- Currently running pipelines
SELECT target_table, batch_started_at, 
       datediff(minute, batch_started_at, current_timestamp()) as running_minutes
FROM gold.etl_control
WHERE batch_status = 'RUNNING';

-- Failed pipelines in last 24 hours
SELECT target_table, source_table, batch_started_at, error_message
FROM gold.etl_control
WHERE batch_status = 'FAILED'
  AND batch_completed_at > current_timestamp() - INTERVAL 24 HOURS;

-- Average load times by table (SLA monitoring)
SELECT target_table,
       avg(datediff(second, batch_started_at, batch_completed_at)) as avg_seconds,
       max(datediff(second, batch_started_at, batch_completed_at)) as max_seconds,
       count(*) as run_count
FROM gold.etl_control
WHERE batch_status = 'SUCCESS'
GROUP BY target_table
ORDER BY avg_seconds DESC;

-- Time-travel: What was the state at midnight?
SELECT * FROM gold.etl_control TIMESTAMP AS OF '2025-01-01 00:00:00'
WHERE target_table = 'gold.dim_customer';
```

### Concurrency Safety

The ETL control table is **partitioned by (target_table, source_table)** which means:

- Each pipeline writes to its own partition
- No contention between parallel pipelines
- Delta's optimistic concurrency handles edge cases

---

## Error Handling & Retry

### Error Classification

Errors are classified into two categories:

| Type | Retry? | Examples |
|------|--------|----------|
| **Retriable** | ✅ Yes | Table busy, concurrent modification, transient Spark errors |
| **Non-Retriable** | ❌ No | Bad SQL, schema mismatch, missing config |

### Using `run_with_retry()`

For ad-hoc notebook testing with automatic retry:

```python
import os
from kimball import Orchestrator

os.environ["KIMBALL_ETL_SCHEMA"] = "gold"

# Retry up to 3 times with exponential backoff
result = Orchestrator("config.yml").run_with_retry(
    max_retries=3,
    backoff_seconds=30  # 30s, 60s, 120s
)
```

### Databricks Jobs Retry (Recommended)

For production, configure retry in the job definition:

```yaml
tasks:
  - task_key: load_dim_customer
    max_retries: 3
    min_retry_interval_millis: 30000  # 30 seconds
    retry_on_timeout: true
    notebook_task:
      notebook_path: /pipelines/run_kimball_pipeline
      base_parameters:
        config_file: "configs/dim_customer.yml"
```

---

## Best Practices

### 1. Use Databricks Jobs for Production

The `PipelineExecutor` is great for development, but production workloads should use Databricks Jobs:
- Native monitoring and alerting
- Visual DAG representation
- Built-in retry logic
- Better resource management

### 2. Set Appropriate Parallelism

```python
# For notebooks: match to cluster size
executor = PipelineExecutor(..., max_workers=4)

# For jobs: use for_each concurrency
for_each_task:
  concurrency: 4  # Match to cluster capacity
```

### 3. Monitor the ETL Control Table

Set up alerts on:
- Pipelines stuck in `RUNNING` state for too long
- Failed pipelines
- SLA breaches (duration > threshold)

### 4. Handle Partial Failures

The framework's watermark system ensures exactly-once processing:

```
Scenario: dim_customer succeeds, dim_product fails
Re-run behavior:
  1. dim_customer detects it's already at latest version → skips
  2. dim_product re-processes from last successful watermark
```

### 5. Use `dry_run()` Before Execution

```python
# Preview what will run without executing
executor.dry_run()
```

---

## Migration from Sequential to Parallel

If you have existing sequential pipelines:

```python
# Before (sequential)
Orchestrator("dim_customer.yml").run()
Orchestrator("dim_product.yml").run()
Orchestrator("fact_sales.yml").run()

# After (parallel with executor)
import os
from kimball import PipelineExecutor

os.environ["KIMBALL_ETL_SCHEMA"] = "gold"

executor = PipelineExecutor([
    "dim_customer.yml",
    "dim_product.yml", 
    "fact_sales.yml"
])

executor.run()
```

The framework automatically:
1. Categorizes pipelines into dimension/fact waves
2. Runs dimensions in parallel
3. Waits for all dimensions to complete
4. Runs facts in parallel
