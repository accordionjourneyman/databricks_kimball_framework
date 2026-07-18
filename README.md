# Databricks Kimball Framework

[![CI](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/ci.yml/badge.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/ci.yml)
[![Integration Tests](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/integration.yml/badge.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/integration.yml)
[![Benchmarks](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/benchmarks.yml/badge.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/benchmarks.yml)
[![codecov](https://codecov.io/gh/accordionjourneyman/databricks_kimball_framework/branch/main/graph/badge.svg)](https://codecov.io/gh/accordionjourneyman/databricks_kimball_framework)
[![License](https://img.shields.io/github/license/accordionjourneyman/databricks_kimball_framework.svg)](https://github.com/accordionjourneyman/databricks_kimball_framework/blob/master/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-261230.svg)](https://github.com/astral-sh/ruff)

A declarative Kimball Gold-layer loading framework for Databricks and Delta
Lake. It compiles strict YAML into a dependency DAG, executes dimensional
loads, and records supplier-contract and data-quality evidence. It assumes
usable source tables already exist; it is not a Bronze ingestion platform or a
complete lakehouse governance plane.

## Requirements

- **Databricks Runtime**: 17.0 LTS or higher (Spark 4.0 / Delta 4.x)
- **Python**: 3.10-3.12
- **Java** for local Spark testing: JDK 17 with `JAVA_HOME` set
- **Delta Lake**: provided by Databricks Runtime, or `delta-spark>=4.0,<5.0` locally

> **Note**: PySpark and Delta Lake are not installed by `pip install .`; they
> are provided by Databricks Runtime. For local development, use `pip install ".[dev]"`.

## Quick start

```bash
pip install .
kimball validate --config examples/configs --target dev
kimball compile --config examples/configs --target dev --output manifest.json
kimball plan --config examples/configs --target dev --against deployed-manifest.json
```

Select `dev`, `test`, or `prod` from `kimball.targets.yml`. Targets contain only
catalog, schema, and checkpoint settings; workspace identity belongs in the
Databricks Asset Bundle target or CI secrets.

## Features

- SCD Types 1, 2, 4, and 6; CDF-driven incremental processing
- Explicit dependency DAG with cycle, missing-upstream, and writer-conflict checks
- Pinned ODCS 3.1 contracts, runtime DQ checks, temporal checks, and durable evidence
- Fact grain, additivity, role-playing, degenerate, and managed junk-dimension metadata
- YAML-managed table and column descriptions
- Keyed HMAC-SHA-256 tokenization, masking, nulling, and dropping PII controls
- Structured Streaming and single-writer compensating rollback (see limitations)

## Project structure

```
src/kimball/  core framework package
tests/        unit and real-Delta integration suites
examples/     demo notebook and YAML configurations
docs/         operational and design documentation
```

## Documentation

- [Getting Started](docs/GETTING_STARTED.md) - canonical installation and first-pipeline guide
- [Configuration](docs/CONFIGURATION.md)
- [Data Supplier Contracts](docs/DATA_CONTRACTS.md)
- [Production Readiness](docs/PRODUCTION_READINESS.md)
- [Framework Features vs SQL Patterns](docs/SQL_PATTERNS.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Streaming CDF](docs/STREAMING.md)
- [Known Limitations](KNOWN_LIMITATIONS.md)

## Usage

```python
from kimball import Orchestrator

Orchestrator("examples/configs/dim_customer.yml").run()
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
