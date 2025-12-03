# Getting Started with Databricks Kimball Framework

This guide will walk you through setting up and running your first Kimball pipeline.

## Prerequisites

- Databricks workspace (DBR 11.3+)
- Python 3.8+
- Delta Lake enabled

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd databricks_kimball_framework
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install the framework in editable mode:
```bash
pip install -e .
```

## Your First Pipeline

### Step 1: Prepare Source Data

Create a source table with Change Data Feed enabled:

```sql
CREATE TABLE dev_silver.customers (
  customer_id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  address STRING,
  updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);

INSERT INTO dev_silver.customers VALUES
  (1, 'Alice', 'Smith', 'alice@example.com', '123 Main St', current_timestamp()),
  (2, 'Bob', 'Jones', 'bob@example.com', '456 Oak Ave', current_timestamp());
```

### Step 2: Create Configuration

Create `my_configs/dim_customer.yml`:

```yaml
table_name: dev_gold.dim_customer
table_type: dimension
scd_type: 2

keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]

surrogate_key_strategy: hash
track_history_columns: [first_name, last_name, email, address]

sources:
  - name: dev_silver.customers
    alias: c
    cdc_strategy: cdf

transformation_sql: |
  SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.address,
    c.updated_at
  FROM c

audit_columns: true
```

### Step 3: Run the Pipeline

```python
from kimball.orchestrator import Orchestrator
from pyspark.sql import SparkSession

# Initialize Spark (if not in Databricks notebook)
spark = SparkSession.builder.getOrCreate()

# Run the pipeline
orchestrator = Orchestrator("my_configs/dim_customer.yml", spark)
orchestrator.run()
```

### Step 4: Verify Results

```sql
SELECT * FROM dev_gold.dim_customer;
```

You should see:
- 2 rows with `__is_current = true`
- Surrogate keys generated
- SCD2 columns populated

### Step 5: Test Incremental Load

Update a customer:

```sql
UPDATE dev_silver.customers
SET address = '789 Elm St'
WHERE customer_id = 1;
```

Run the pipeline again:

```python
orchestrator.run()
```

Verify SCD2 behavior:

```sql
SELECT customer_id, address, __is_current, __valid_from, __valid_to
FROM dev_gold.dim_customer
WHERE customer_id = 1
ORDER BY __valid_from;
```

You should see:
- Old row with `__is_current = false` and `__valid_to` set
- New row with `__is_current = true` and updated address

## Next Steps

- Explore the `examples/` directory for more complex scenarios
- Read `README.md` for full configuration reference
- Check `learned_lessons.md` for architectural insights

## Common Issues

**Pipeline runs but no data appears:**
- Verify source table exists and has data
- Check that CDF is enabled on source table
- Review Spark logs for errors

**"Table not found" error:**
- Ensure you're using the correct catalog/schema names
- Verify table names match your environment (dev/prod)

**Watermark not advancing:**
- Check source table has new versions: `DESCRIBE HISTORY source_table`
- Verify transformation SQL doesn't filter out all rows
