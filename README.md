# Databricks Kimball Framework

A configurable ETL framework implementing Kimball dimensional modeling patterns on Databricks with Delta Lake.

## Implementations

This repository provides multiple implementations of the same Kimball patterns:

| Implementation                          | Language | Best For                                      |
| --------------------------------------- | -------- | --------------------------------------------- |
| [**PySpark**](implementations/pyspark/) | Python   | Production Databricks workloads, full control |
| [**dbt**](implementations/dbt/)         | SQL      | SQL-first teams, simpler configurations       |

## Quick Start

### PySpark Implementation

```bash
cd implementations/pyspark
pip install .
```

Then open `examples/Kimball_Demo.py` in Databricks.

### dbt Implementation

```bash
cd implementations/dbt
dbt deps
dbt run
```

Then open `examples/dbt_Kimball_Demo.py` in Databricks.

## Features

Both implementations support:

- **SCD Type 1**: Overwrite in place
- **SCD Type 2**: Track history with valid_from/valid_to
- **Surrogate Keys**: Hash or identity-based
- **Change Data Feed (CDF)**: Incremental processing
- **Foreign Key Lookups**: With Kimball-style defaults (-1 for unknown)
- **Performance Optimized**: Configurable "lite" validations vs "strict" dev checks

## Documentation

See [docs/](docs/) for detailed documentation:

- [Getting Started](docs/GETTING_STARTED.md)
- [Configuration](docs/CONFIGURATION.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Known Limitations (PySpark)](implementations/pyspark/KNOWN_LIMITATIONS.md)

## Benchmarking

Both demos collect timing metrics for comparison. Run both demos and check the benchmark output at the end of each notebook.

## License

Apache 2.0 - See [LICENSE](implementations/pyspark/LICENSE)
