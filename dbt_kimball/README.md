# dbt Kimball Framework

A dbt-based implementation of the Kimball dimensional modeling framework for Databricks.

## Features

- **SCD Type 2** via dbt's native `snapshots` (check strategy)
- **SCD Type 1** via `incremental` models with `unique_key`
- **Fact tables** with temporal joins to SCD2 dimensions
- **Kimball defaults** for unknown dimensions (-1, -2, -3)
- **Databricks optimized** with Delta Lake

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
cd dbt_kimball
dbt deps
```

### 4. Run Pipeline

```bash
# Run SCD2 snapshots first
dbt snapshot

# Then run models
dbt run
```

## Project Structure

```
dbt_kimball/
├── models/
│   ├── staging/      # Bronze → Silver transforms
│   ├── dimensions/   # SCD1 dimensions (incremental)
│   └── facts/        # Fact tables with FK lookups
├── snapshots/        # SCD2 dimensions (dbt native)
├── macros/           # Kimball utilities
└── examples/         # Demo notebook
```

## Comparison: dbt vs PySpark Framework

| Feature       | PySpark      | dbt                |
| ------------- | ------------ | ------------------ |
| SCD2          | Custom MERGE | Native snapshots   |
| SCD1          | Custom MERGE | Incremental models |
| Orchestration | Custom       | dbt-core           |
| Testing       | Manual       | Built-in           |
| Documentation | Manual       | Auto-generated     |

## Running on Databricks

Use the demo notebook at `examples/dbt_Kimball_Demo.py` to run the full pipeline with verification tests.
