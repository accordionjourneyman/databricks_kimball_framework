# Polars Kimball Framework

Pure Polars + Delta Lake implementation of Kimball dimensional modeling patterns.

## Features

- **SCD Type 1**: Delta MERGE for in-place updates
- **SCD Type 2**: Proper history tracking with valid_from/valid_to
- **CDF Reader**: Read Change Data Feed via PyArrow bridge
- **Watermark Tracking**: Track processed Delta versions
- **Fact Table Helpers**: FK lookups with Kimball-style defaults (-1 for unknown)

## Installation

```bash
pip install -e .
```

## Usage

```python
from kimball_polars import apply_scd1, apply_scd2, read_cdf

# SCD2 - creates history rows
dim_customer = apply_scd2(
    target_path="gold/dim_customer",
    source_df=customers_df,
    natural_keys=["customer_id"],
    track_columns=["first_name", "last_name", "email", "address"],
    surrogate_key="customer_sk"
)

# SCD1 - uses Delta MERGE
dim_product = apply_scd1(
    target_path="gold/dim_product",
    source_df=products_df,
    natural_keys=["product_id"],
    surrogate_key="product_sk"
)

# Read CDF for incremental processing
changes = read_cdf("silver/customers", starting_version=5)
```

## ⚠️ Rally Track Constraints

This is the "rally track" - fast but requires careful handling:

### 1. Single Writer Model

```python
# ❌ Don't do this - concurrent writers without locking
# Process A: write_delta("table", df1)
# Process B: write_delta("table", df2)  # Conflict!

# ✅ Do this - serialize writes or use DynamoDB lock
```

### 2. Memory Wall

```python
# ❌ Don't load giant CDF batches
changes = dt.load_cdf(starting_version=0)  # 200GB in RAM

# ✅ Batch by version
for v in range(start, end, batch_size):
    changes = dt.load_cdf(starting_version=v, ending_version=v+batch_size)
```

### 3. Manual Maintenance

```python
from deltalake import DeltaTable

# Must run periodically:
dt = DeltaTable("path/to/table")
dt.optimize.compact()              # Compaction
dt.vacuum(retention_hours=168)     # Cleanup
```

## When to Use

| Use Case               | Polars        | PySpark         |
| ---------------------- | ------------- | --------------- |
| Local development      | ✅ Fast       | ❌ Slow startup |
| Single-node ETL        | ✅            | ⚠️ Overkill     |
| Distributed processing | ❌            | ✅              |
| Databricks production  | ⚠️            | ✅ Native       |
| Concurrent writers     | ⚠️ Needs lock | ✅ Built-in     |

## Demo

```bash
python examples/Kimball_Demo.py
```

See `examples/Kimball_Demo.py` for a complete walkthrough.
