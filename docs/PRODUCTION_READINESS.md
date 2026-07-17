# Production readiness

This project is a Kimball Gold-layer loading framework for Delta Lake. It is
not an ingestion platform, a semantic layer, or a complete lakehouse control
plane. It assumes governed source tables already exist and focuses on safe,
declarative dimension and fact processing.

## Required delivery gates

The repository CI is the product contract, not an optional example. A change
must pass:

1. Python compilation, Ruff lint, and Ruff format checks.
2. Mypy for the Spark-free control plane and Pyright for the full package.
3. Unit tests with branch coverage of at least 70 percent.
4. Real Spark/Delta integration tests in the Java 17 Docker image.
5. Wheel build and Databricks Asset Bundle validation where credentials exist.
6. ODCS producer/consumer compatibility checks.
7. CodeQL, dependency review, `pip-audit`, Gitleaks, and Dependabot review.

New behavior is developed test-first. A correctness fix requires a regression
that fails before the fix. Spark/Delta semantics require a real-Delta test in
addition to mocks.

## Compile before execution

Use the CLI in CI and before deployment:

```bash
kimball validate configs --profile production
kimball compile configs --profile production --output manifest.json
kimball plan configs --profile production --previous deployed-manifest.json
kimball bundle configs --profile production --output databricks.yml
```

Production compilation requires every inferred in-project dependency to be
listed in `depends_on`. The compiler rejects cycles, missing declared
upstreams, duplicate target writers, and conflicting auxiliary writers such as
two pipelines managing the same junk/history table. The plan classifies
metadata-only, validation, backfill, and breaking changes, then reports the
transitively affected downstream tables.

In-process execution is serial because separate Python threads are not Spark
session isolation. Generated Databricks Jobs express DAG levels as tasks and
are the supported parallel execution boundary. Generated jobs enforce one
concurrent run; each target remains a strict single-writer resource.

## Runtime evidence

The framework writes:

| Table | Evidence |
| --- | --- |
| `etl_control` | Source watermarks, batch state, fingerprints, and row metrics. |
| `etl_data_quality_events` | Schema, CDC, freshness, DQ, late-event, and ordering findings. |
| `etl_contract_temporal_state` | Per-contract, per-business-key event-time and CDF-version maxima. |
| `etl_contract_registry` | Accepted producer contract versions and canonical document digests. |

Compatible scalar DQ rules share one aggregation. Uniqueness uses one action
per key set. `validation.mode` can be `full`, `sampled`, or `approximate`, with
explicit sample/action budgets. Validation metrics expose action count and
duration so contract cost can be compared with transformation cost.

Temporal state is staged during validation and committed only after the target
merge and watermark succeed. This applies to batch and streaming micro-batches.
Replay is monotonic and idempotent. `observability.write_failure: warn` retains
the ETL result and logs the exception type if evidence storage is unavailable;
`error` fails the run. Validation failures themselves are never swallowed.

## Transaction and recovery contract

A target Delta `MERGE` is atomic for that target. The target and `etl_control`
are separate Delta transactions. The recovery mechanism is therefore a
single-writer compensating rollback protocol, not cross-table ACID or an
"atomic batch transaction."

Recovery requires retained Delta history and commit metadata. It is not safe
with concurrent writers or after required history has been vacuumed. Serverless
environments that cannot tag commits do not have automatic zombie recovery.
See [Known Limitations](../KNOWN_LIMITATIONS.md).

## Security contract

PII equality encoding and security pseudonymization are distinct:

- `tokenize` uses keyed HMAC-SHA-256 and resolves the key from an explicit
  `env://NAME` or `databricks://scope/key` reference at runtime.
- `fast_hash` uses unsalted `xxhash64`; it is only a fast analytical encoding
  and does not protect low-entropy values from dictionary attacks.
- `hash` is a deprecated alias for `fast_hash` and emits a warning.
- `mask`, `null`, and `drop` implement display reduction/data minimization.

Secrets and secret values are excluded from manifests and error messages.
Use Databricks secret scopes in deployed jobs; environment references are
intended for controlled local/CI environments.

## Deployment boundary

The base wheel contains Spark-free configuration, contracts, planning, and CLI
APIs. Runtime processing needs Databricks Runtime or the `spark` extra. Local
integration uses Spark 4.x, Delta 4.x, and Java 17. Generated bundles provide
jobs, dependencies, single-run concurrency, and rollback-friendly artifacts;
workspace promotion, approvals, blue/green policy, and data backup remain the
deploying organization's responsibility.
