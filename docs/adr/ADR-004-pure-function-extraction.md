# ADR-004: pure-function extraction for the data plane, oracle-guarded

Status: accepted
Date: 2026-09-02

## Context

The two highest-defect-density modules of this cycle — `processing/scd2.py`
(single-pass SCD2 MERGE) and `processing/key_broker.py` (set-based FK
resolution) — mix three concerns in one body: **staging algebra**
(partitioning source commits into EXPIRE/HYDRATE/INSERT buckets), **key
derivation** (Type 7 durable keys, fingerprints, sentinel SKs), and **Delta
writes** (conditional MERGE builders). Four of this cycle's five production
bugs lived in that mixture, and each was a *plan-shape coupling* failure:
the code computed which MERGE branches to emit from eager `isEmpty()`
inspections, so a wrong intermediate predicate silently changed the write
plan instead of raising.

Two structural facts made the bugs cheap to fix but expensive to find:

1. The unit suite (846 tests) is mock-based; the MERGE plan was never
   observable in it. Only the golden dataset (`tests/golden/`, run in the
   Databricks workflow against real Delta) caught every one of the five bugs.
2. The staging algebra itself is deterministic and pure — its inputs are
   DataFrames and config, its output a staged frame — but it is only
   reachable through the write path, so it can only be exercised with a
   live JVM today.

The Kimball invariants are **not** negotiable inputs to any refactor:

- surrogate keys are deterministic hashes of (natural key, effective time);
- reserved sentinel SKs (-1 MISSING, -2 BAD VALUE, -3 NOT APPLICABLE,
 -4 INFERRED) are the only negative SKs and are never generated;
- every dimension member row carries provenance (`__key_origin`,
  `__member_status`, `__is_skeleton`) and hydration overwrites it;
- Type 7 durable keys and fingerprints are derived exactly once per
  (natural key, effective time) and stamped identically by insert rows,
  skeleton placeholders, and hydration rows (`stamp_type7_columns`);
- watermarks advance only on success, batch identity lives in Delta
  `userMetadata`, pre-image CDF rows are filtered before staging.

## Decision

Extract the **staging algebra and key derivation** into pure, JVM-free
modules; keep the **write layer** boring and unchanged.

1. `processing/staging.py` (new): `filter_cdf_deletes`,
   `rank_source_versions`, `classify_actions` — pure DataFrame-in /
   DataFrame-out functions with no DeltaTable, no MERGE builder, no
   `get_spark()` call. `scd2._merge_single_pass` becomes an orchestrator:
   stage → derive → hand to the (unchanged) conditional MERGE builder.
2. **Key derivation seam — already satisfied.** During implementation,
   `processing/key_generator.py` was audited and found to *already* meet
   the pure-seam contract (101 lines; `type7_key_columns`,
   `stamp_type7_columns`, `HashKeyGenerator`, `_canonical_payload`;
   DataFrame/Column-in, DataFrame/Column-out; no DeltaTable, no
   `get_spark`). Sentinel constants already live in
   `common/constants.py` (`DEFAULT_MEMBERS`, `RESERVED_DIMENSION_KEYS`).
   Creating a separate `key_derivation.py` would duplicate, not extract;
   so this step is recorded as *audited and satisfied in place* rather
   than moved.
3. `table_creator.py`: the three SQL-safety regexes and DDL string
   assembly move behind a typed `ColumnSpec` model and one serializer;
   a property-style test asserts no `;`, `--`, or `USING` token can be
   smuggled through identifier/type slots.
4. `validation.py`: the twelve try/except + `_build_test_result` blocks
   collapse into one `run_check(name, severity, callable)` combinator.
   The `details` strings are preserved verbatim — they are operator-
   facing contract, not decoration.

The config layer, watermark/transaction layer, and the golden suite are
**explicitly out of scope**. Compacting them trades auditability for line
count, which is anti-Kimball.

## Oracle guardrails (binding for every step)

Each extraction step must satisfy, in order:

1. **Capture first, refactor second.** Before touching a module, the
   existing tests (unit + golden + integration) must be green on the
   untouched code. Those suites are the oracle; they are never edited in
   the same commit as the refactor that must pass them.
2. **No oracle edits in refactor commits.** If a test fails after a pure
   extraction, the extraction is wrong. Changing an expectation to make
   an extraction pass is a semantic change and needs its own commit,
   rationale, and review.
3. **Golden is the highest authority.** A green unit suite with a red
   golden suite blocks the step. The Databricks workflow run is the
   release gate, not a follow-up.
4. **Coverage floor holds.** Branch coverage over `tests/unit` stays
   >= 80% (the CI gate); extraction may not hide untestable code behind
   seams — that is regression, not refactoring.

## Consequences

Positive:

- Staging algebra becomes unit-testable without a JVM; the next
  `isEmpty()`-style plan coupling bug is caught at the seam instead of in
  a Photon stack trace.
- Key derivation has one derivation point (already true in code since
  `stamp_type7_columns`; the move makes it structural).
- The write layer stays deliberately boring: conditional MERGE builders
  emit every branch with data-independent conditions.

Negative / accepted costs:

- One indirection layer between orchestration and Delta; stack traces
  gain a frame.
- Re-export shims must be tracked for removal once callers migrate.
- The refactor's own commits carry no behavior change by construction —
  which means review attention must go to the diff, not the test diff,
  precisely because the test diff is empty.

## Verification

- `pytest tests/unit` green before and after each step (CI-exact scope:
  `--ignore=tests/benchmarks --ignore=tests/unit/test_performance.py
  --cov=src/kimball --cov-branch --cov-fail-under=80`).
- `pytest tests/golden/test_golden.py` green in the Docker image after
  each step touching the data plane.
- `tests/integration/test_scd2_single_pass_regression.py` and
  `test_scd7_key_broker.py` green after steps 1–2.
- `ruff check`, `ruff format --check`, `mypy src` clean after each step.