# Databricks Kimball Framework

A declarative, CDF-based ETL framework implementing Kimball dimensional modeling patterns on Databricks with Delta Lake.

## Requirements

- **Databricks Runtime**: 13.3 LTS or higher
- **Python**: 3.10+
- **Delta Lake**: Provided by Databricks Runtime (or `delta-spark>=2.4.0` for local dev)

> **Note**: PySpark and Delta Lake are _not_ installed by `pip install .` — they're provided by the Databricks Runtime. For local development, use `pip install ".[dev]"`.

## Quick Start

```bash
pip install .
```

Then open `examples/Kimball_Demo.py` in Databricks.

## Features

- **SCD Type 1**: Overwrite in place
- **SCD Type 2**: Track history with valid_from/valid_to
- **Surrogate Keys**: Hash or identity-based (Delta Identity Columns)
- **Change Data Feed (CDF)**: Incremental processing with watermark tracking
- **Foreign Key Lookups**: With Kimball-style defaults (-1 for unknown)
- **Performance Optimized**: Configurable "lite" validations vs "strict" dev checks
- **Crash Recovery**: Transactional batch recovery — see [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md#2-crash-consistency-atomic-batch-recovery)

## Project Structure

```
├── src/kimball/           # Core framework code
│   ├── common/            # Config, errors, utilities
│   ├── orchestration/     # Orchestrator, watermarks, executor
│   ├── processing/        # Loader, merger, key generators
│   └── observability/     # Bus matrix, resilience features
├── tests/                 # Unit and integration tests
├── examples/              # Demo notebook and YAML configs
└── docs/                  # Detailed documentation
```

## Documentation

- [Getting Started](docs/GETTING_STARTED.md)
- [Configuration](docs/CONFIGURATION.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Known Limitations](KNOWN_LIMITATIONS.md)

## Usage

```python
import os
from kimball import Orchestrator

# Configure ETL schema
os.environ["KIMBALL_ETL_SCHEMA"] = "gold"

# Run a dimension pipeline
Orchestrator("examples/configs/dim_customer.yml").run()
```

## License

Apache 2.0 - See [LICENSE](LICENSE)
