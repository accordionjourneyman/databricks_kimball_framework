# Contributing

Thank you for your interest in contributing. This project follows
standard open-source practices: clear issues, focused PRs, and
respectful collaboration.

## Quick start

```bash
git clone https://github.com/accordionjourneyman/databricks_kimball_framework.git
cd databricks_kimball_framework

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -e ".[dev]"
```

## Project layout

```
src/kimball/          # Framework source code
  common/             # Config models, errors, utilities
  contracts/          # ODCS contract handling
  orchestration/      # Pipeline orchestration
  processing/         # Core ETL (SCD, keys, merge)
  streaming/          # Streaming CDF support
  observability/      # Monitoring, bus matrix
  planning/           # DAG, manifest, bundle
tests/
  unit/               # Fast, no external dependencies
  integration/        # Requires local Spark + Delta
  benchmarks/         # Performance benchmarks
  golden/             # End-to-end correctness tests
docs/                 # User-facing documentation
examples/             # Reference pipelines
tools/                # CLI utilities for operators
```

## Development commands

| Task                         | Command                                         |
| ---------------------------- | ----------------------------------------------- |
| Unit tests                   | `pytest tests/unit/`                            |
| Integration tests (local)    | `pytest tests/integration/`                     |
| Benchmarks                   | `pytest tests/benchmarks/ --benchmark-only`     |
| All tests                    | `pytest tests/`                                 |
| Lint                         | `ruff check src/ tests/ tools/`                 |
| Format                       | `ruff format src/ tests/ tools/`                |
| Type check (control plane)   | `mypy src/kimball/common/ src/kimball/contracts/ src/kimball/planning/ src/kimball/cli.py` |
| Type check (full)            | `pyright src/kimball/`                          |
| Build wheel                  | `python -m build --wheel`                       |

Unit tests run without Spark. Integration tests require a local
Spark session (JDK 17) or a Databricks Connect connection.

## PR expectations

1. **One logical change per PR.** A PR that fixes a typo and refactors
   a module is two PRs.

2. **Tests added or updated for behavior changes.** A bug fix without
   a regression test will be asked for. A new feature without tests
   will be asked for.

3. **CHANGELOG.md updated** under the `## Unreleased` section.

4. **Documentation updated** if you change public APIs. The schema
   reference is `docs/CONFIGURATION.md`; add your change there.

5. **No secrets, tokens, or hardcoded credentials** in diffs. Use
   `env://NAME` or `databricks://scope/key` references.

6. **Backward compatibility considered.** If your change breaks
   existing configs, note it in the PR description. Breaking changes
   are accepted during the beta phase with a deprecation note.

7. **Pass the production-readiness checklist:**
   - [ ] `pytest tests/unit/` passes locally
   - [ ] `ruff check src/ tests/` passes
   - [ ] `mypy src/kimball/common/ src/kimball/cli.py` passes
   - [ ] `CHANGELOG.md` updated
   - [ ] Docs updated if public API changed

## Issue-first policy

For non-trivial changes (new feature, large refactor), **open an
issue first** and discuss the approach. This avoids wasted work on
PRs that don't align with the project's direction.

Good issue types:
- Bug report with minimal reproduction
- Feature request with use case
- Documentation gap with specific page
- Performance regression with benchmark

## Commit message format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): short description

Longer explanation if needed.

Fixes #123
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`, `ci`.

## Code style

- Line length: 88 (configured in `pyproject.toml`)
- Imports: absolute, sorted by `ruff`
- Docstrings: Google style for modules, NumPy style for functions
- Type hints: required for public functions, optional for internal
- No wildcard imports
- Prefer explicit over clever

## Testing guidelines

This project uses **three testing patterns**, each for a different
situation.  The goal is to maximise real execution and minimise
mock-wiring tests that assert only that mocks were called.

### Pattern A: Real Spark with inline data (preferred)

Create a small DataFrame inline with ``spark.createDataFrame()``,
call the function under test, and assert on the actual output.

```python
def test_hashdiff_deterministic(spark: SparkSession):
    df = spark.createDataFrame([("a", "x"), ("a", "y")], ["name", "city"])
    rows = df.withColumn("h", compute_hashdiff(["name", "city"])).select("h").collect()
    assert rows[0]["h"] != rows[1]["h"]
```

**When to use:** Functions that transform DataFrames or compute values
(hashing, key generation, validation, PII masking, SCD merge logic).

**Requires:** The ``spark`` fixture from ``tests/unit/conftest.py``,
which creates a local SparkSession.  It skips automatically when Java
is unavailable or on Databricks Connect, so the test never blocks CI.

**Anti-pattern:** Do NOT mock ``xxhash64``, ``col``, ``lit``, or other
Spark internals to test a hash function.  Every mocked Spark function
turns a correctness test into a mock-wiring test.

### Pattern B: Fake providers (ops/ package)

For the ``ops/`` package (inspect, recover, explain, deploy), tests
use fake implementations of the abstract provider protocols:

```python
control = FakeControl(exists=True, batches=(batch("b1", "silver.s", "SUCCESS", 5),))
history = FakeHistory(exists=True, current_version=3, commits=(commit(3, "b1"),))
```

**When to use:** Testing decision logic that depends on provider
output (state reconciliation, recovery planning, deploy pre-flight).

**Fakes live in:** ``tests/unit/ops/fakes.py``.

### Pattern C: Pure mock (discouraged)

```python
@patch("kimball.processing.scd2.xxhash64")
@patch("kimball.processing.scd2.col")
@patch("kimball.processing.scd2.DeltaTable")
def test_scd2_merge(self, mock_dt, ...):
    ...
    mock_dt.merge.assert_called_once()
```

**This pattern is strongly discouraged.**  Tests that mock the core
logic they claim to test are tautologies — they pass even when the
real implementation is broken, as long as the mock call pattern
doesn't change.  Prefer Pattern A whenever possible.

**Exception:** Mocking only external I/O (``get_spark``, SparkSession
construction, filesystem access) is acceptable when the function
under test has no interesting data transformations.

### Shared test data (``tests/data/``)

Small CSV files with stable, version-controlled content:

```
tests/data/
  customers.csv        # 3 customers, standard customer dimension
  orders.csv           # 3 orders, standard transaction fact
  ...
```

Load them in any test with:

```python
from tests.data import customer_fixture

def test_something(spark: SparkSession):
    df = customer_fixture(spark)
    ...
```

Or via direct path:

```python
df = spark.read.csv("tests/data/customers.csv", header=True)
```

Add a new fixture file when:
- The same data set is needed by 2+ tests
- The data set has clear Kimball semantics (dimension, fact, staging)
- The file is under 20 rows (above that, generate in a script)

### Test directory layout

```
tests/
  unit/               # Fast unit tests (Pattern A or B)
  integration/        # Full pipeline against local Delta
  data/               # Shared CSV fixtures (Pattern A feeds)
  benchmarks/         # pytest-benchmark performance tests
  golden/             # Snapshot / golden-file comparison tests
  unit/ops/fakes.py   # Fake providers for ops/ package
```

### Checklist for writing a good test

- [ ] Uses real ``spark.createDataFrame()`` for input data
- [ ] Asserts on actual output values (not mock call counts)
- [ ] Input data is minimal (2-5 rows, as many columns as needed)
- [ ] Covers the happy path
- [ ] Covers at least one edge case (null, empty, duplicate)
- [ ] Test data shared across tests lives in ``tests/data/``
- [ ] Does NOT mock the function's own internals

## Reporting bugs

Use the bug report issue template. Include:

- `kimball --version` output
- Python, PySpark, Delta versions
- Minimal config to reproduce
- Full traceback
- Relevant logs

## Security issues

**Do not open a public issue.** See `SECURITY.md` for the disclosure
process.
