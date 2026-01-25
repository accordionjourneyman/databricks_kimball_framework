# Kimball Framework Usage Guide

## 1. Setup

Install the library in your Databricks cluster or local environment:

```bash
pip install .
```

## 2. Configuration

Create a YAML configuration file for your Fact table (e.g., `configs/fact_sales.yml`).

```yaml
table_name: {{ env }}_gold.fact_sales
table_type: fact
keys:
  surrogate_key: sales_sk
  natural_keys: [transaction_id]

sources:
  - name: {{ env }}_silver.transactions
    alias: t
    cdc_strategy: cdf
  - name: {{ env }}_gold.dim_customer
    alias: c
    cdc_strategy: full # Dimensions are usually small enough or we just need current state

transformation_sql: |
  SELECT
    t.transaction_id,
    t.amount,
    c.customer_sk,
    t.transaction_date
  FROM t
  JOIN c ON t.customer_id = c.customer_id

delete_strategy: hard
audit_columns: true
```

## 3. Execution

Run the pipeline in a Databricks Notebook:

```python
from kimball import Orchestrator
import os

# Set environment variables for Jinja2 templating
os.environ["env"] = "prod"

# Initialize and Run
orchestrator = Orchestrator("configs/fact_sales.yml")
orchestrator.run()
```

## 4. Bus Matrix

Generate the Enterprise Bus Matrix documentation:

```python
from kimball import generate_bus_matrix

print(generate_bus_matrix("configs/"))
```

## 5. Testing

**Prerequisites:** Ensure `JAVA_HOME` is set for PySpark:

```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

Run unit tests:

```bash
pytest tests/
```
