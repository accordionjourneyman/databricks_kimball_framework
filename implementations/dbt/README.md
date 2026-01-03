# dbt Kimball Framework

A dbt-based implementation of the Kimball dimensional modeling framework for Databricks.

## Features

- **SCD Type 2** via dbt's native `snapshots` (check strategy)
- **SCD Type 1** via `incremental` models with `unique_key`
- **Fact tables** with temporal joins to SCD2 dimensions
- **Kimball defaults** for unknown dimensions (-1, -2, -3)
- **Databricks optimized** with Delta Lake
- **CDF Support** via Delta `table_changes()` with timestamp fallback
- **Watermark Management** via `etl_control` table
- **Batch Auditing** with lifecycle tracking (start/complete/fail)
- **Query Metrics** via Databricks Query History API

## Quick Start

### 1. Install dbt-databricks

```bash
pip install dbt-databricks
```

### 2. Configure Profile

Copy `profiles.yml.example` to `~/.dbt/profiles.yml` and update:

```yaml
databricks:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: main
      schema: demo_gold
      host: "your-workspace.cloud.databricks.com"
      http_path: "/sql/1.0/warehouses/your-warehouse-id"
      token: "your-token"
```

### 3. Install Dependencies

```bash
cd implementations/dbt
dbt deps
```

### 4. Run Pipeline

```bash
# Run SCD2 snapshots first
dbt snapshot

# Then run models (CDF mode by default)
dbt run

# Or disable CDF for timestamp fallback
dbt run --vars '{"use_cdf": false}'
```

## Project Structure

```
implementations/dbt/
├── models/
│   ├── staging/      # CDF-enabled source transforms
│   ├── dimensions/   # SCD1 dimensions with batch hooks
│   ├── facts/        # Fact tables with FK lookups
│   └── meta/         # etl_control table
├── snapshots/        # SCD2 dimensions (dbt native)
├── macros/
│   ├── watermark.sql         # get/update watermark, batch lifecycle
│   ├── load_cdf.sql          # Delta CDF loading
│   ├── deduplicate_cdf.sql   # Multi-update handling
│   └── ...
└── examples/         # Demo notebook with metrics
```

## Comparison: dbt vs PySpark Framework

| Feature        | PySpark                 | dbt                             |
| -------------- | ----------------------- | ------------------------------- |
| SCD2           | Custom MERGE            | Native snapshots                |
| SCD1           | Custom MERGE            | Incremental models              |
| CDF Support    | Native                  | ✅ via `table_changes()` macro  |
| Watermarks     | `ETLControlManager`     | ✅ `etl_control` model + macros |
| Batch Auditing | Built-in                | ✅ pre/post hooks               |
| Query Metrics  | `QueryMetricsCollector` | ✅ Query History API            |
| Orchestration  | Custom                  | dbt-core                        |
| Testing        | Manual                  | Built-in                        |

## Running on Databricks

Use the demo notebook at `examples/dbt_Kimball_Demo.py` to run the full pipeline with:

- Day 1/Day 2 incremental loading
- SCD2 and SCD1 verification tests
- Watermark tracking verification
- Query metrics collection
