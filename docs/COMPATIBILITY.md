# Compatibility Matrix

The framework is currently in beta (0.x). We support the current
major.minor version (0.2.x) only. Retro-compatibility is not
guaranteed until the project exits beta phase.

## Tested combinations (version 0.2.0)

Each row below is a combination that passes the full unit test
suite in CI. The Python matrix runs on every PR; the Spark/Delta
combinations run on every push and pull request via the
`integration` job in the `ci` workflow.

| Python | PySpark | Delta Lake | Databricks Runtime | Status | CI evidence |
| ------ | ------- | ---------- | ------------------ | ------ | ----------- |
| 3.10   | 4.0.1   | 4.2.0      | DBR 17.0 LTS       | ✅     | [lint-and-unit job](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/ci.yml) |
| 3.11   | 4.0.1   | 4.2.0      | DBR 17.0 LTS       | ✅     | [lint-and-unit job](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/ci.yml) |
| 3.12   | 4.0.1   | 4.2.0      | DBR 17.0 LTS       | ✅     | [lint-and-unit job](https://github.com/accordionjourneyman/databricks_kimball_framework/actions/workflows/ci.yml) |

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
