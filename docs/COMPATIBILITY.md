# Compatibility Matrix

The framework is in beta. Package metadata currently reports version 0.3.0;
compatibility statements below describe the combinations exercised by the
repository workflows. They do not imply that every Python version runs every
Spark or Databricks combination.

## CI coverage for version 0.3.0

| Workflow path | Python | Runtime | Coverage |
| --- | --- | --- | --- |
| Fast unit lane | 3.10, 3.11, 3.12 | No JVM | Lint, type checks, config validation, and non-Spark unit tests |
| Spark unit lane | 3.11 | Docker image with PySpark 4.0.1 and Delta 4.2.0 | Spark-marked unit tests against local Spark and Delta |
| Databricks compatibility | 3.11 | Workspace selected by CI environment | Golden tests, scheduled and manually dispatchable |

See the [CI workflow](../.github/workflows/ci.yml) and
[Databricks integration workflow](../.github/workflows/integration.yml) for the
current commands and triggers. The configured Databricks Runtime is external to
the repository and is not pinned by this matrix.

## Optional dependencies

| Extra     | Purpose                       | Declared range (pyproject.toml)    |
| --------- | ----------------------------- | --------------------------------- |
| `spark`   | Local PySpark + Delta         | pyspark>=4.0.0,<4.3.0; delta-spark>=4.0.0,<5.0.0 |
| `remote`  | Databricks Connect            | databricks-connect>=16.0.0,<17.0.0; databricks-sdk>=0.30.0 |
| `dev`     | Tests, lint, type check, build | latest stable                    |

## Unsupported combinations

The following are **known to fail or untested**:

- Python < 3.10 (uses `match` statements and `| None` syntax)
- PySpark < 4.0 (relies on `pyspark.errors.PySparkException`)
- Delta Lake < 4.0 (no `PERSIST` restrictions, different MERGE semantics)
- Databricks Connect < 16.0 (different serverless contract)

## Reporting a compatibility issue

Open a bug report with the issue template. Include:

- `kimball --version` output
- Full output of `pip show pyspark delta-spark databricks-connect`
- The exact config that fails
- The full traceback

We will add your combination to the matrix above if we can reproduce
and fix the issue.
