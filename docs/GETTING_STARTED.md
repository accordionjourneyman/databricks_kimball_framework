# Getting Started with Databricks Kimball Framework

This guide will walk you through setting up and running your first Kimball pipeline.

## Prerequisites

- Databricks workspace (DBR 13+)
- Python 3.10+
- Delta Lake enabled

## Installation

1. Clone the repository:

```bash
git clone https://github.com/your-username/databricks_kimball_framework.git
cd databricks_kimball_framework
```

2. Install the framework:

```bash
pip install .
```

Or in editable mode for development:

```bash
pip install -e ".[dev]"
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

surrogate_key_strategy: identity  # SCD2 requires identity strategy
track_history_columns: [first_name, last_name, email, address]

sources:
  - name: dev_silver.customers
    alias: c
    cdc_strategy: cdf
    primary_keys: [customer_id] # Required for CDF deduplication

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
import os
from kimball import Orchestrator

# Configure ETL schema once at notebook start
os.environ["KIMBALL_ETL_SCHEMA"] = "dev_gold"

# Run the pipeline
Orchestrator("my_configs/dim_customer.yml").run()
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

## Common Issues

**Pipeline runs but no data appears:**

- Verify source table exists and has data
- Check that CDF is enabled on source table
- Review Spark logs for errors

**"ETL schema must be specified" error:**

- Set `KIMBALL_ETL_SCHEMA` environment variable before running:
  ```python
  import os
  os.environ["KIMBALL_ETL_SCHEMA"] = "dev_gold"
  ```

**"Table not found" error:**

- Ensure you're using the correct catalog/schema names
- Verify table names match your environment (dev/prod)

**Watermark not advancing:**

- Check source table has new versions: `DESCRIBE HISTORY source_table`
- Verify transformation SQL doesn't filter out all rows
