# Databricks Kimball Framework

[![CI](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/ci.yml/badge.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/ci.yml)
[![Integration Tests](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/integration.yml/badge.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/integration.yml)
[![Benchmarks](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/benchmarks.yml/badge.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/benchmarks.yml)
[![codecov](https://codecov.io/gh/accordionjourneyman/databricks_kimball_framework/branch/main/graph/badge.svg)](https://codecov.io/gh/accordionjourneyman/databricks_kimball_framework)
[![License](https://img.shields.io/github/license/accordionjourneyman/databricks_kimball_framework.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/blob/master/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-261230.svg)](https://github.com/astral-sh/ruff)
[![Tests](https://img.shields.io/badge/tests-97%20unit%20%2B%2022%20integration%20%2B%2010%20benchmark-brightgreen.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions)

A declarative Kimball Gold-layer loading framework for Databricks and Delta
Lake. It compiles strict YAML into a dependency DAG, executes dimensional
loads, and records supplier-contract and data-quality evidence. It assumes
usable source tables already exist; it is not a Bronze ingestion platform or a
complete lakehouse governance plane.

## Requirements
- **Databricks Runtime**: 17.0 LTS or higher (required for Spark 4.0 / Delta 4.x)
- **Python**: 3.10-3.12
- **Java** (for local Spark testing): JDK 17 with `JAVA_HOME` set
  ```bash
  # Ubuntu/Debian
  sudo apt install openjdk-17-jdk
  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
  ```
- **Delta Lake**: Provided by Databricks Runtime (or `delta-spark>=4.0,<5.0` for local dev)

> **Note**: PySpark and Delta Lake are _not_ installed by `pip install .` â€” they're provided by the Databricks Runtime. For local development, use `pip install ".[dev]"`.

## Quick Start

```bash
pip install .
```

Then open `examples/Kimball_Demo.py` in Databricks.
For the streaming variant, open `examples/Kimball_Streaming_Demo.py`.

Compile and review a project before execution:

```bash
kimball validate examples/configs --profile production
kimball compile examples/configs --profile production --output manifest.json
kimball plan examples/configs --previous deployed-manifest.json
```

## Features

- **SCD Type 1**: Overwrite in place
- **SCD Type 2**: Track history with valid_from/valid_to
- **Surrogate Keys**: Hash or identity-based (Delta Identity Columns)
- **Change Data Feed (CDF)**: Incremental processing with watermark tracking
- **Dependency Planning**: Explicit DAG, cycle/missing-upstream/writer-conflict checks, deterministic manifests, downstream-aware diffs, and Databricks Job generation
- **Data Contracts**: Pinned ODCS 3.1 producer/consumer CI gates plus runtime schema, CDC, freshness, DQ, and temporal checks
- **Operational Evidence**: Durable DQ events, cross-batch event-time state, validation-cost metrics, and optional webhooks
- **Structured Streaming**: Per-source streaming via `StreamingOrchestrator` â€” same config, same SQL, same merger
- **Foreign Key Lookups**: With Kimball-style defaults (-1 for unknown)
- **Kimball Modeling Metadata**: Fact patterns, grain, measure additivity, role-playing, degenerate, and managed junk dimensions
- **Catalog Documentation**: Diff-aware YAML table and column descriptions, including removal of previously YAML-owned comments
- **PII Controls**: Keyed HMAC-SHA-256 tokenization, explicitly non-security `fast_hash`, masking, nulling, and dropping
- **Performance Optimized**: Configurable "lite" validations vs "strict" dev checks
- **Crash Recovery**: Single-writer compensating rollback â€” see [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md#2-crash-consistency-single-writer-compensating-rollback)

## Project Structure

```
â”œâ”€â”€ src/kimball/           # Core framework code
â”‚   â”œâ”€â”€ common/            # Config, errors, utilities
â”‚   â”œâ”€â”€ orchestration/     # Orchestrator, watermarks, executor
â”‚   â”œâ”€â”€ processing/        # Loader, merger, key generators
â”‚   â”œâ”€â”€ streaming/         # Streaming CDF orchestrator, loader, checkpoint
â”‚   â””â”€â”€ observability/     # Bus matrix, resilience features
â”œâ”€â”€ tests/                 # Unit and integration tests
â”œâ”€â”€ examples/              # Demo notebook and YAML configs
â””â”€â”€ docs/                  # Detailed documentation
```

## Documentation

- [Getting Started](docs/GETTING_STARTED.md)
- [Configuration](docs/CONFIGURATION.md)
- [Data Supplier Contracts](docs/DATA_CONTRACTS.md)
- [Production Readiness](docs/PRODUCTION_READINESS.md)
- [Framework Features vs SQL Patterns](docs/SQL_PATTERNS.md)
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
