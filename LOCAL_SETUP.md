# Local Setup Guide

This guide gets the Kimball Framework running with a **local Spark + Delta Lake** environment for fast test iteration and debugging — no Databricks cluster required.

## Prerequisites

- **Docker Desktop** (with Compose) — used to run the containerized Spark+Delta+Java environment
- **Git** — to clone the repo

> **Note:** You do **not** need Java or PySpark installed on your host machine. The Docker container provides OpenJDK 17, PySpark 4.0, and Delta Spark 4.2.

## Quick Start (Docker — recommended)

### 1. Clone and enter the repo

```bash
git clone <your-repo-url> databricks_kimball_framework
cd databricks_kimball_framework
```

### 2. Build the Docker image

```bash
docker-compose build
```

This builds a Python 3.11 image with OpenJDK 17, PySpark 4.0.1, and Delta Spark 4.2.0. The first build takes ~2-3 minutes; subsequent builds are cached.

> **Important:** The Docker image deliberately does **not** install `databricks-connect`. When `databricks-connect` is installed alongside `pyspark`, it hijacks `SparkSession.builder.getOrCreate()` and refuses to create local sessions. By keeping them separate, the Docker environment gets a clean PySpark + Delta setup.

### 3. Run all unit tests (fast, no Spark needed)

```bash
docker-compose run --rm kimball-tests python -m pytest tests/unit/ -q
```

Expected: 67 tests pass in ~1 second.

### 4. Run integration tests (real Delta tables, local Spark)

```bash
docker-compose run --rm kimball-tests python -m pytest tests/integration/test_scd_integration.py -v
```

Expected: 4 tests pass in ~100 seconds. These create real Delta tables, run SCD1/SCD2 merges, and verify data integrity.

### 5. Run everything (unit + integration)

```bash
docker-compose run --rm kimball-tests python -m pytest tests/unit/ tests/integration/test_scd_integration.py -v
```

Expected: 71 tests pass in ~110 seconds.

## Using the test runner script

The `tools/run_tests.py` script provides a `-t` flag to select the execution target.

```bash
# Inside Docker (recommended):
docker-compose run --rm kimball-tests python tools/run_tests.py -t local
docker-compose run --rm kimball-tests python tools/run_tests.py -t local --integration
docker-compose run --rm kimball-tests python tools/run_tests.py -t local -k test_scd2

# Against a remote Databricks cluster (requires credentials in .env):
docker-compose run --rm -e KIMBALL_TARGET=databricks kimball-tests python tools/run_tests.py -t databricks
```

### Test runner flags

| Flag | Description |
|------|-------------|
| `-t local` | Use local Spark+Delta (default) |
| `-t databricks` | Use Databricks Connect (requires `.env` with `DATABRICKS_HOST` and `DATABRICKS_TOKEN`) |
| `-k <keyword>` | pytest keyword filter (e.g., `-k test_scd2`) |
| `--integration` | Include `tests/integration/` |
| `--unit-only` | Only `tests/unit/` (default) |
| `-v` | Verbose output |
| `-x` | Stop on first failure |
| `--lf` | Re-run only last failed tests |

## What the integration tests cover

| Test | Description |
|------|-------------|
| `test_scd1_initial_load_and_update` | SCD1: Initial load → update source → verify in-place update, SK preserved |
| `test_scd2_initial_load_and_change` | SCD2: Initial load → change tracked column → verify new version created, old expired |
| `test_scd2_full_snapshot_delete_detection` | SCD2: Full snapshot → delete row from source → verify anti-join detects and expires |
| `test_scd2_add_new_column` | Schema evolution: Add column to source + config → verify target schema evolves |

## How local Spark works (target reconciliation)

The framework detects whether it's running on Databricks or local Spark via `_is_databricks_runtime()` (checks for `DATABRICKS_HOST`, `DATABRICKS_RUNTIME_VERSION`, or `SPARK_REMOTE` env vars). The following features gracefully degrade on local Spark:

| Feature | Databricks | Local Spark |
|---------|------------|-------------|
| **Surrogate keys** | `xxhash64(natural_keys)` / `xxhash64(natural_keys + __valid_from)` for SCD2 (deterministic, distributed-safe) | Same (hash strategy works everywhere) |
| **Liquid Clustering** | `CLUSTER BY (columns)` | `PARTITIONED BY (columns)` |
| **Schema evolution** | `delta.schema.autoMerge.enabled` | Manual `ALTER TABLE ADD COLUMN` before MERGE |
| **Deletion Vectors** | Enabled via TBLPROPERTIES | Skipped (not supported) |
| **Predictive Optimization** | Enabled via TBLPROPERTIES | Skipped (not supported) |
| **SK NOT NULL constraint** | Enforced (IDENTITY guarantees values) | Skipped (hash fallback may produce NULLs on edge cases) |

This means the **same test code** runs on both targets without modification — the framework adapts automatically.

## Native (non-Docker) setup

If you prefer to run tests natively on your host (without Docker):

### Prerequisites

- **Python 3.11**
- **Java 17** (OpenJDK recommended) — set `JAVA_HOME` env var
- **pip**

### Steps

```bash
# 1. Clone
git clone <your-repo-url> databricks_kimball_framework
cd databricks_kimball_framework

# 2. Create virtual environment
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

# 3. Install with dev dependencies
# IMPORTANT: Do NOT install databricks-connect alongside pyspark
# for local testing — it hijacks SparkSession.builder.
pip install pyspark==4.0.1 delta-spark==4.2.0 pyyaml jinja2 pydantic pytest ruff
pip install --no-deps -e .

# 4. Run unit tests
python -m pytest tests/unit/ -q

# 5. Run integration tests (requires Java)
python tools/run_tests.py -t local --integration
```

> **Windows users:** Install Java via `winget install Microsoft.OpenJDK.17` and set `JAVA_HOME` to `C:\Program Files\Microsoft\jdk-17.0.x.x-hotspot\` (check exact path after install).

> **If `databricks-connect` is already installed:** The `conftest.py` has a fallback that monkey-patches `SparkSession.builder.getOrCreate()` to bypass the Databricks Connect override. However, this is fragile — Docker is recommended.

## Databricks target (remote testing)

For testing against a real Databricks workspace:

### 1. Create a `.env` file

```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXX
DATABRICKS_CLUSTER_ID=XXXX-XXXXXX-XXXXXXX
```

### 2. Run

```bash
# Native (requires databricks-connect installed):
pip install databricks-connect
python tools/run_tests.py -t databricks --integration

# Docker (mounts .env automatically):
docker-compose run --rm -e KIMBALL_TARGET=databricks kimball-tests python tools/run_tests.py -t databricks
```

## Project structure

```
databricks_kimball_framework/
├── src/kimball/          # Framework source code
│   ├── processing/       # SCD strategies, loaders, mergers, key generators
│   ├── orchestration/     # Orchestrator, watermark, transactions
│   ├── common/            # Config, constants, utilities, runtime detection
│   └── observability/     # Bus matrix, resilience, metrics
├── tests/
│   ├── unit/              # Mock-based unit tests (fast, no Spark needed)
│   ├── integration/       # End-to-end tests with real Delta tables
│   └── conftest.py        # Pytest fixtures (SparkSession, target selection)
├── tools/
│   └── run_tests.py       # Test runner with -t local / -t databricks
├── Dockerfile             # Local Spark+Delta+Java environment (no databricks-connect)
├── docker-compose.yml     # Docker Compose config
└── pyproject.toml         # Project config and dependencies
```

## Troubleshooting

### "Java gateway process exited before sending its port number"

Java is not installed or `JAVA_HOME` is not set. Either:
- Use Docker (recommended): `docker-compose run --rm kimball-tests python -m pytest tests/integration/test_scd_integration.py -v`
- Install Java 17 natively and set `JAVA_HOME`

### "Only remote Spark sessions using Databricks Connect are supported"

The `databricks-connect` package hijacks `SparkSession.builder`. Either:
- Use Docker (which doesn't install `databricks-connect`)
- Or the `conftest.py` fallback should handle it. If it doesn't, uninstall `databricks-connect` for local testing: `pip uninstall databricks-connect`

### Integration tests fail with "PARSE_SYNTAX_ERROR at 'BY'"

This means `CLUSTER BY` (Liquid Clustering) is being used on local Spark, which doesn't support it. The framework should automatically fall back to `PARTITIONED BY`. If you see this error, ensure the `_is_databricks_runtime()` function in `table_creator.py` is returning `False` on local Spark.

### Integration tests fail with "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"

The SK NOT NULL constraint is being enforced on local Spark where IDENTITY columns aren't available. The framework should skip this constraint on local Spark. Check that `_is_databricks_runtime()` returns `False` and the `apply_basic_constraints` method skips the constraint.

### "Multiple source rows matched" in SCD2 merge

This means the SCD2 merge is seeing duplicate keys in the staged source. On local Spark with hash-based SKs, this can happen if:
1. Default rows (-1, -2, -3) are being detected as deleted by the anti-join (fixed: filter by natural key > 0)
2. The `HashKeyGenerator` now includes `__valid_from` for SCD2 so each version gets a unique SK