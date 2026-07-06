# Databricks Kimball Framework

[![CI](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/ci.yml/badge.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/ci.yml)
[![Integration Tests](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/integration.yml/badge.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/integration.yml)
[![Benchmarks](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/benchmarks.yml/badge.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/benchmarks.yml)
[![codecov](https://codecov.io/gh/accordionjourneyman/databricks_kimball_framework/branch/main/graph/badge.svg)](https://codecov.io/gh/accordionjourneyman/databricks_kimball_framework)
[![License](https://img.shields.io/github/license/accordionjourneyman/databricks_kimball_framework.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/blob/master/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-261230.svg)](https://github.com/astral-sh/ruff)
[![Tests](https://img.shields.io/badge/tests-97%20unit%20%2B%2022%20integration%20%2B%2010%20benchmark-brightgreen.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions)

A declarative, CDF-based ETL framework implementing Kimball dimensional modeling patterns on Databricks with Delta Lake.

## Requirements
- **Databricks Runtime**: 17.0 LTS or higher (required for Spark 4.0 / Delta 4.x)
- **Python**: 3.10+
- **Java** (for local testing): JDK 8+ with `JAVA_HOME` set
  ```bash
  # Ubuntu/Debian
  sudo apt install openjdk-11-jdk
  export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
  ```
- **Delta Lake**: Provided by Databricks Runtime (or `delta-spark>=2.4.0` for local dev)

> **Note**: PySpark and Delta Lake are _not_ installed by `pip install .` — they're provided by the Databricks Runtime. For local development, use `pip install ".[dev]"`.

## Quick Start

```bash
pip install .
```

Then open `examples/Kimball_Demo.py` in Databricks.
For the streaming variant, open `examples/Kimball_Streaming_Demo.py`.

## Features

- **SCD Type 1**: Overwrite in place
- **SCD Type 2**: Track history with valid_from/valid_to
- **Surrogate Keys**: Hash or identity-based (Delta Identity Columns)
- **Change Data Feed (CDF)**: Incremental processing with watermark tracking
- **Structured Streaming**: Per-source streaming via `StreamingOrchestrator` — same config, same SQL, same merger
- **Foreign Key Lookups**: With Kimball-style defaults (-1 for unknown)
- **Performance Optimized**: Configurable "lite" validations vs "strict" dev checks
- **Crash Recovery**: Transactional batch recovery — see [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md#2-crash-consistency-atomic-batch-recovery)

## Project Structure

```
├── src/kimball/           # Core framework code
│   ├── common/            # Config, errors, utilities
│   ├── orchestration/     # Orchestrator, watermarks, executor
│   ├── processing/        # Loader, merger, key generators
│   ├── streaming/         # Streaming CDF orchestrator, loader, checkpoint
│   └── observability/     # Bus matrix, resilience features
├── tests/                 # Unit and integration tests
├── examples/              # Demo notebook and YAML configs
└── docs/                  # Detailed documentation
```

## Documentation

- [Getting Started](docs/GETTING_STARTED.md)
- [Configuration](docs/CONFIGURATION.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Streaming CDF](docs/STREAMING.md)
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

Licensed under the Apache License, Version 2.0. See the `LICENSE` file for details.
