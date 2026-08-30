# ADR-003: model-integrity validator in the spark-free control plane

Status: accepted
Date: 2026-08-30

## Context

The framework validates models at two independent layers today:

1. **Per-table, config-shaped.** `TableConfig.validate_kimball_rules()` rejects
   invalid single tables at load time (surrogate/natural key requirements, SCD
   key/effective-at constraints, FK role-playing shape, append/append_only
   combinations, junk/degenerate conflicts).
2. **Project-wide, DAG-shaped.** `ProjectCompiler.compile()` rejects structural
   graph faults: unreachable upstreams, cycles, duplicate target writers,
   auxiliary-table conflicts, missing upstreams in production.

Neither layer expresses **cross-table modeling intent**. The following defect
classes pass both checks and are only discovered as data damage in a downstream
environment:

- a column name reused with different meaning or type across tables
  (`COLUMN_SEMANTICS_CONFLICT`),
- dimension attributes denormalized into fact tables
  (`FACT_DIMENSION_ATTRIBUTE`),
- a fact whose FK does not resolve to the referenced dimension's declared key
  (`GRAIN_KEY_MISMATCH`, `ORPHAN_REFERENCE`),
- incremental sources without a resumable basis, or delete strategies that can
  strand downstream facts (`INCREMENTAL_LOAD_FRAGILE`),
- FKs pointing at non-dimensions or at tables outside the project
  (`MISSING_REFERENCE_TARGET`),
- tables and columns that ship undocumented while the framework already owns
  a YAML→Delta comment pipeline (`MISSING_DESCRIPTION`).

These checks only need the configuration graph; executing SQL or touching
 Spark cannot contribute a signal that the declared metadata does not already
contain.

Sourcery's review output (rule-id grouping with explicit auto-fixability) was
adopted as the reporting model for this validator after a live triage
demonstrated both the value (deterministic findings grouped by rule with a
fixability axis) and the danger (an auto-fix suggestion that broke type
narrowing; low-code-quality noise on intentionally long MERGE builders).

## Decision

### 1. Location and runtime profile

A new `src/kimball/planning/model_integrity.py` module, invoked from
`ProjectCompiler.compile()` after the existing per-node loop and before the
`ProjectValidationError` raise. The module operates on a **normalized project
graph** (`ProjectGraph`), not raw configs: nodes with their typed
`TableConfig`, typed FK edges (`ForeignKeyEdge`: pipeline, column,
references, dimension_key, relationship, durable_column, identity_map), a
declared-downstream map, and column-description/contract-type indexes. The
graph is built once per compilation; each rule is a small query over it — the
dbt-manifest shape (nodes + edges), kept fully typed because several rules
need edge semantics a generic DAG would erase (type7/durable keys,
identity_map resolution, role-playing).

The module imports only stdlib and `kimball.common.config` — the same import
profile as `compiler.py`, `manifest.py`, and `bundle.py`.

This keeps the entire model-integrity path **Spark-free** and JVM-free: it runs
in the CI `lint-and-unit` job, on a laptop, and in `kimball validate` regardless
of the execution target. The existing
`tests/unit/test_spark_free_imports.py` guarded-import tripwire is extended to
cover the new module, making the boundary mechanically enforced rather than
aspirational.

Rejected alternative: a Spark-backed validator that executes
`transformation_sql` to inspect real output columns. It would catch strictly
more (e.g. undeclared pass-through columns) but would bind model review to a
Databricks/Delta runtime, exclude local and CI execution, and duplicate signals
the runtime `DataQualityValidator` already produces on actual data. The
static/dynamic split is deliberate: static for model shape, runtime for data
content.

### 2. Issue model, not exception model

Findings are `ProjectIssue` values — the existing compiler currency — extended
with two fields:

- `fixability: Literal["auto_fixable", "suggest_fix", "decision_required"]`
- `fix: FixSuggestion | None` — a dataclass payload
  (`{field, old, new}` or `{field, candidates: [...]}`), present only for the
  first two values.

The engine (`check_project`) is a pure function of declared metadata and
**reports everything** — it does not consult `modeling_exceptions`. Suppression
lives entirely in the compiler, which resolves the waiving table (the finding's
anchor table first, then any project table listing the same `(code, column)`;
cross-table findings such as description drift can legitimately be waived from
either side of the divergence). A waived error emits an
`EXCEPTION_APPROVED` warning naming the waiving table and its
`decision_ref`, so an exemption is never silent. The summary line counts both
live issues and exception-approved waivers.

`auto_fixable` issues never mutate configuration in this build. The compiler
reports; a future opt-in `kimball model fix` command may apply them after the
classification has been validated against real models. This is the lesson of
the Sourcery triage: a mis-classified auto-fix silently corrupts intent, while
a wrong *suggestion* costs seconds. Mutation is deferred until the
classification earns it.

### 3. Fixability axis

One question classifies every rule: **can the correct fix be derived from
already-declared metadata alone?**

- `auto_fixable` — deterministic; exactly one correct value exists (e.g. FK
  `dimension_key` not matching the referenced dimension's only declared
  surrogate/durable key).
- `suggest_fix` — the machine can draft a fix but a choice of intent remains
  (e.g. proposing `non_additive_dimensions` from measure keys; proposing
  candidate `primary_keys` for an incremental source).
- `decision_required` — the finding *is* a modeling choice; the only exits are
  editing the model or recording an exception.

Classification lives in the check functions, not in a central table, because
each rule knows what evidence its fix needs. A rule's fixability may itself be
conditional (e.g. `MISSING_REFERENCE_TARGET` is `auto_fixable` on a unique
near-match candidate, `suggest_fix` when candidates are zero or many).

### 4. Exceptions are typed, per-table, and reference a decision

Intentional deviations are declared in the flagged table's own YAML:

```yaml
modeling_exceptions:
  - code: FACT_DIMENSION_ATTRIBUTE   # must be a known code
    columns: [customer_name]
    reason: denormalized for dashboard latency; see dashboard spike
    decision_ref: phase2/evidence/ADR-002
```

Pydantic validates the shape (unknown codes, blank reasons, empty columns and
duplicate `(code, column)` pairs are hard errors). Suppression requires an
exact match on `(code, table, column)`; suppressed errors emit an
`EXCEPTION_APPROVED` warning carrying the `decision_ref`, so an exemption is
never silent and remains greppable in `validate`/`plan` output.

Rejected alternatives: inline `# noqa`-style directives (untyped, unfindable,
easy to abuse); a single central ledger file (exceptions drift away from the
model they annotate and need a second loader).

### 5. Gating follows the established severity idiom

Model-integrity findings use the profile-based severity already used for
`UNDECLARED_DEPENDENCY`: warnings in `dev`, errors in `test`/`production`,
forcing `kimball validate --target prod`, `plan`, and bundle generation to fail
in CI. A `--strict` flag promotes warnings to errors in every profile. No new
command or gate is introduced; the compile path already fails on error
severity.

### 6. Reporting shape

`kimball validate` / `plan` output gains a Sourcery-style aggregation: a
one-line summary (count by severity and fixability, plus exception-approved
waivers) plus a group-by table over `(code, fixability)`. `decision_required`
findings name the ledger path; the message tells the modeler the two exits —
fix the model, or file an exception with a `decision_ref`.

### 7. Documentation coverage is a model-integrity rule

The framework already owns catalog documentation end-to-end:
`table_description`/`column_descriptions` in YAML are synchronized to Delta
comments by `DescriptionManager` after each merge, diffed against a
`kimball.descriptions.manifest` table property. What was missing is
**enforcement**: nothing failed when a table or column was undocumented, and 7
of 9 example configs shipped without descriptions — precisely the gap dbt
closes with docs coverage.

`MISSING_DOCUMENTATION` closes it as a normal integrity check: a table
without `table_description` is `decision_required`; a declared column
(natural/SK/durable keys and FK outputs are excused — surrogate plumbing
needs no narrative) missing from `column_descriptions` is `suggest_fix` with
the missing column named. Consequence taken seriously: **configurations that
omit `column_descriptions` are no longer silently under-validated** — the
`FACT_DIMENSION_ATTRIBUTE` and `COLUMN_SEMANTICS_CONFLICT` rules read those
descriptions as their signal, so documentation coverage and model-governance
coverage are the same knob. Example configs ship fully documented.

## Consequences

- Model-review latency drops from "run a pipeline / open a notebook" to a
  sub-second, Spark-free CLI call suitable for pre-commit and CI.
- The spark-free import tripwire must be extended in the same change as the
  validator — a PR adding Spark imports to `model_integrity.py` should fail CI,
  not a review.
- `column_descriptions` becomes load-bearing: several checks derive column
  semantics from it, so configs without descriptions silently lose those
  checks. Documentation should present descriptions as the unlock for model
  governance, not an optional nicety.
- Fix suggestions are advisory and the `fix` payload shape becomes public API
  for future tooling (`kimball model fix`); renaming it later is a breaking
  change and should be treated that way.
- The exception ledger creates a governance surface: exceptions are committed
  artifacts, visible in `validate`/`plan` output and code review, never
  environment-level toggles.
- Findings are static: a config that passes `compile()` is declared-shape
  correct, not provably correct at the data level; runtime validators
  (`DataQualityValidator`, grain sampling) keep their role unchanged.
- **PII work (in flight, not yet merged)** will add enforcement on top of
  `PIIPolicy` (`pii.columns[].strategy`). It should integrate through this
  validator rather than a parallel gate: the natural extension is a
  `PII_POLICY_GAP` rule over `ProjectGraph` (a column whose description
  references PII, or a source-contract column marked sensitive, without a
  `pii` policy entry — or vice versa). The graph and issue plumbing already
  carry everything that rule needs; nothing in this design changes when it
  lands. Until merged, descriptions mentioning PII are advisory markers only.

## Verification plan

- `tests/unit/test_model_integrity.py` — one happy-path, one violation, one
  exception-suppression case per rule; fixability assertions per rule;
  run on a JVM-less host as the zero-Spark acceptance test.
- Guarded-import test extended to the new module (`spark_loaded == False`).
- A negative example config per rule confirms production-profile errors are
  raised and suppressed only by a matching exception.
- Example configs ship with `table_description`/`column_descriptions` on every
  table, so the CI `kimball validate --target prod` step exercises the
  documentation rule against realistic models.

## References

- `docs/ARCHITECTURE.md` — compile-time control plane
- `tools/benchmark_metrics.py` / Sourcery triage (session log, 2026-08-30) —
  reporting-shape and auto-fixability precedent
- `phase2/evidence/ADR-002-controlled-dbt-assessment.md` — ADR conventions
- Kimball & Ross, *The Data Warehouse Toolkit*, 3rd ed. — conformance, grain,
  and fact/dimension attribute placement rules that motivate the check set