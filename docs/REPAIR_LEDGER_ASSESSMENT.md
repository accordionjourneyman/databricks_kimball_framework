**Repair ledger: evidence and design assessment — 27 September 2026**

This assessment evaluates a normalized patch/repair ledger against comparable implementations, recent research, failure scenarios, and operator effort. It also records the first implementation now present in the working tree. Repository observations refer to the current working tree based on commit `80f15f1501fdf90090223789d4e3d8cee298b6cd`, including uncommitted changes; they are not a claim about a released wheel or every deployment.

**Implementation status [VERIFIED].** The working tree now includes `kimball repair plan`, `apply`, `status`, and `rollback`; a normalized Delta ledger; dependency-aware source snapshot rebuild; elapsed-time and commit records; and guarded whole-table rollback with source-cursor rewind. The supported rebuild is currently batch Delta input to SCD1 targets. This is a first strategy, not a general repair engine. Its writes are in place and sequential across tables; `--writers-stopped` records an operator acknowledgement but does not acquire a lock. Unit tests cover the new paths, but no live Spark/Delta repair experiment has been run.

**Recommendation [INFERRED].** Keep the normalized ledger as the historical record for repair intent, reads, attempts, commits, and recovery. The first implementation adopts this design for one snapshot-rebuild strategy. Continue using Delta for native commits and retained snapshots; add other replay strategies only with strategy-specific correctness evidence. Precise row provenance can remain an optional later capability.

One earlier recommendation needs qualification: a saved `version_before` is a candidate restore point. It is not automatically a safe undo operation. If version 41 precedes a bad patch at 42 and a valid update arrives at 43, restoring 41 removes both 42 and 43. Immediate rollback, compensating correction, and recomputation must be distinct plan strategies.

The following are documented capabilities of comparable systems. The adoption decisions are this assessment's inferences, not statements that those products prescribe this framework's schema.

| Evidence | Documented mechanism [VERIFIED] | Adoption decision [INFERRED] |
|---|---|---|
| [SQLMesh plans](https://github.com/SQLMesh/sqlmesh/blob/main/docs/concepts/plans.md) | Restatements select models and time intervals and cascade to downstream models. Certain model kinds, including SCD2 and self-referential models, have restricted bounded backfills. | The current planner computes configured dependency closure for full target rebuilds. Time-bounded restatements remain future work; keep model-kind eligibility explicit and never infer replay safety from a MERGE key alone. |
| [SQLMesh audits](https://sqlmesh.readthedocs.io/en/latest/concepts/audits/) | Blocking audits stop propagation. Plans can validate isolated replacement tables before promotion; ordinary production runs can already have written invalid rows before an audit fails. | The current repair rebuild is in place and sequential. Isolated replacement and validation before publication are possible future capabilities; a successful validation query alone does not establish reader isolation. |
| [lakeFS branch protection](https://docs.lakefs.io/v1.71/howto/protect-branches/) and [revert](https://docs.lakefs.io/reference/python/branches/) | Protected branches accept merges with validation hooks; reverting records a new commit. | Borrow isolated build, validation, publication, and attributable compensation. Do not assume an object-store branch automatically coordinates Unity Catalog, streaming checkpoints, or external sinks. |
| [OpenLineage run cycle](https://openlineage.io/docs/spec/run-cycle/), [dataset versions](https://openlineage.io/docs/spec/facets/dataset-facets/version_facet/), and [quality assertions](https://openlineage.io/docs/spec/facets/dataset-facets/data_quality_assertions/) | Runs have lifecycle events, inputs/outputs can carry versions, and assertion outcomes attach to datasets. | Align identifiers and events with this vocabulary; optionally export it. OpenLineage is interoperability/observability metadata, not a transaction coordinator or row-level undo engine. |
| [Delta history and restore](https://docs.delta.io/delta-utility/) | Table history provides commit metadata and operation-dependent metrics. RESTORE creates data-changing log entries; downstream streams can reprocess restored files. | The repair path records actual commits and candidate restore versions in its ledger. Coordinate downstream processing explicitly; restoring table data does not restore all pipeline state. |

**Data Vault 2.0 contributes correction and temporal semantics.** Scalefree's [data-quality guidance](https://www.scalefree.com/blog/architecture/data-quality-in-the-data-vault-architecture/) recommends retaining received facts and applying changeable quality/business rules downstream. Its [late-arriving-data guidance](https://www.scalefree.com/knowledge/webinars/data-vault-friday/how-to-deal-with-late-arriving-data/) distinguishes load time from business-effective time and describes rebuilding affected snapshots or applying counter-transactions. These are practitioner recommendations, not evidence that a Data Vault migration is required.

For this framework, preserve the raw input or replayable change archive, and version a correction's meaning separately from its execution. A correction can be recorded today while taking effect for last month's business period. Capture both dates. Use typed correction tables for durable business overrides, including business key, effective interval, correction revision, and supersession link. A one-off transformation bug can instead be addressed by a versioned code artifact plus replay; it does not automatically need a permanent override table. Rebuild derived snapshots and affected dimension histories from the corrected interpretation. The ledger itself can remain normalized relational metadata without hubs, links, or satellites.

**Data mesh contributes ownership and the operating model.** [Zhamak Dehghani's original principles](https://martinfowler.com/articles/data-mesh-principles.html) assign data-product ownership to domains and put common infrastructure and automated governance in the platform. Applied here [INFERRED], the domain owner supplies correction semantics and business checks; the framework supplies planning, execution, lineage, and recovery. Record affected product/contract owners and externally managed downstream consumers. A repair cannot claim ecosystem-wide completion while a dependent product outside the controlled DAG remains unrepaired. Data mesh does not supply a rollback algorithm or require distributed ownership within a small deployment.

Recent research informs the granularity of provenance and state capture, with limited direct applicability:

| Research | Finding and scope [VERIFIED] | Consequence for this design [INFERRED] |
|---|---|---|
| [FaDE: More Than a Million What-ifs Per Second](https://gromitchan.com/assets/papers/mohammed-vldb2025.pdf), PVLDB 18(4), VLDB 2025 | Uses instrumented DuckDB operators and compact relational provenance to evaluate hypothetical deletion/scaling interventions. It is an engine-specific what-if evaluation system. | Useful precedent for optional impact analysis over joins and aggregates. It does not establish that arbitrary PySpark UDFs or Delta repairs support automatic inverses, nor transfer its performance results to this framework. |
| [In-Memory Indexing and Querying of Provenance in Data Preparation Pipelines](https://arxiv.org/html/2511.03480v1), November 2025 preprint | TensProv uses sparse provenance structures and operator semantics, including instrumented joins, in a pandas-based implementation. Evaluation includes three application pipelines and synthetic TPC-DI joins. | If exact row attribution becomes necessary, investigate separate compact indexes rather than repeating all source combinations in each business row. This is not evidence of production Spark scalability; the preprint's deployment and evaluation boundaries matter. |
| [IcedTea](https://www.vldb.org/pvldb/vol18/p902-ni.pdf), PVLDB 18(3), 2024 / VLDB 2025, with [published implementation](https://github.com/texera/icedtea) | The paper's indexed abstract and the Texera artifact describe returning to captured operator states and stepping through prior execution. The complete paper could not be fetched in this session. | Preserve execution state when claiming replay of stateful computation. This supports the distinction between data snapshots and processing state; it supplies no validated generic production rollback implementation for this repository. |

These papers do not recommend this exact repair ledger schema. Their relevant contribution is that provenance, operation semantics, and execution state are separate concerns. None is evidence that a source-version dictionary alone makes arbitrary denormalized updates reversible.

**Existing components and pre-repair baseline.** The first column records what the framework's existing components do; design implications below are updated to reflect the repair path now in the working tree. Conclusions apply to inspected paths, not every deployment.

| Existing component | Observation [VERIFIED] | Design implication [INFERRED] |
|---|---|---|
| [ETL control](../src/kimball/orchestration/watermark.py) | Stores mutable current state per target/source, including batch ID, times, status, row counts, and prior successful watermark. Starting a run replaces prior state. | Continue using it as a cursor. Historical repair intent, attempts, and commits live in separate `etl_repair_*` tables; rollback rewinds only the relevant cursor rows rather than restoring the shared control table wholesale. |
| [Transaction manager](../src/kimball/orchestration/transaction.py) | Provides single-target compensating rollback and batch zombie recovery with single-writer and retained-history requirements. | The repair path adds a separate multi-step protocol with durable pre-repair versions and tagged commit attribution. Its ledger/history checks and repair scope do not turn several Delta writes into one transaction. |
| [Operational recovery](../src/kimball/ops/recover.py), [state reconciliation](../src/kimball/ops/state_reconciler.py), and [repair execution](../src/kimball/ops/repair_execution.py) | `recover` handles failed/orphan batches; the repair path handles completed data work that must be recomputed from corrected inputs. | Use `kimball recover` for crash state and `kimball repair` for semantic source correction. Conflict diagnostics and `--writers-stopped` are not an enforceable distributed lock. |
| [Work plan](../src/kimball/orchestration/services/work_plan.py), [source loader](../src/kimball/orchestration/services/source_loader.py), and [data loader](../src/kimball/processing/loader.py) | Ordinary full reads are not automatically pinned by the standard incremental work plan; CDF reads have bounded versions. | The repair path separately records exact Delta snapshot versions for configured source and lookup reads. Reads from tables rebuilt earlier in the same repair resolve to the produced version at apply time and are saved as actual attempt reads. |
| [Manifest](../src/kimball/planning/manifest.py) and [compiler](../src/kimball/planning/compiler.py) | Manifests contain dependencies, declared writes, config digests, and framework version; changes can be classified as requiring backfill. | Repair freezes the project digest, DAG, configured reads, and managed output set. It rejects undeclared SQL inputs and does not promise to reverse arbitrary external side effects. |
| [Quality events](../src/kimball/observability/data_quality.py) and [repair ledger](../src/kimball/ops/repair_ledger.py) | Quality events are durable; ordinary query metrics are in memory. The repair ledger persists repair events, elapsed milliseconds, reads, and attributed commits. | Review per-step attempt reads, commits, and events for repair evidence. This ledger does not itself prove domain correctness or capture every external side effect. |
| [Full reload](../src/kimball/orchestration/services/recovery.py) | Drops the target and configured SCD4 history table, then resets source watermarks. | A repair promising rollback needs retained backup/publication semantics. A dropped/recreated table must not be treated as the same physical table generation just because its name matches. |

**Normalized repair ledger [VERIFIED].** The implementation separates immutable intent, planned reads, actual attempt reads, output targets, attempts, native commits, and lifecycle events across Delta tables. Normalization helps avoid inconsistent audit facts; it does not make independent Delta writes atomic. Physical generation IDs accompany table names where rollback safety depends on object identity.

| Relation | Grain and principal data |
|---|---|
| `etl_repair_revision` | One correction revision: repair ID/key, reason, owner, optional artifact URI/digest and superseded repair ID. |
| `etl_repair_input_correction` | One corrected source: source table, bad version, selected replacement version, and physical source generation. |
| `etl_repair_plan` | One frozen plan: project/config digests, runtime profile, strategy, and publication policy. |
| `etl_repair_step` | One ordered target rebuild with strategy, scope digest, expected target version, rollback version, and stable operation token. Dependencies are in `etl_repair_step_dependency`. |
| `etl_repair_write` | One output table and write role per step, including whether it existed and its generation/version before the plan. |
| `etl_repair_read` | One planned step input: table, role, read mode, snapshot version or CDF bounds, schema digest, generation, and prior watermark. The current source-rebuild command uses snapshot reads; the ledger schema also represents CDF reads. |
| `etl_repair_attempt` and `etl_repair_step_attempt` | One execution attempt and each attempted step, with actor/environment and observed target existence/version. Retries remain attributable. |
| `etl_repair_attempt_read` | One actual read used during an attempt, including the resolved version for a dependency rebuilt earlier in the same repair. |
| `etl_repair_commit` | One observed Delta commit: target generation/version, before-version, operation token, attribution, rollback version, and commit time. |
| `etl_repair_event` | One lifecycle event with status, time, elapsed milliseconds, and optional error/evidence reference. |

**Further audit design [INFERRED].** Link existing quality-event records to the step attempt and version/scope checked, and version the validation definitions. Store optional engine metrics separately from core relational keys. Wall-clock attempt duration includes waiting/retries where defined; per-operation duration and summed task CPU time are different measures. State the clock and timing definition in exported reports.

**Storage/concurrency caveat [VERIFIED].** Declaring logical primary/foreign keys is not a concurrency guarantee on every Delta deployment. Enforce unique revision IDs, operation tokens, and `(dataset identity, native version)` commit ownership in the execution protocol, and reconcile partial records even where storage constraints are informational.

**Version-vector semantics [VERIFIED].** The current path stores configured source, dimension lookup, identity-map, and junk-dimension versions at the read boundary, rather than on every output business row. Versions of external inputs are pinned independently at plan time; versions of internal dependencies are captured when their upstream repair step has completed. This does not guarantee a transactionally consistent snapshot across independently updated sources. Use an upstream release/batch boundary when business consistency requires one. Explicitly choose whether a repair reconstructs an earlier interpretation or uses current corrected data.

**Execution and recovery acceptance [INFERRED].** The source-rebuild path implements several of these mechanisms, but no live Spark/Delta run has exercised the failure boundaries. Treat this as a production acceptance checklist, not a statement that the system already guarantees all outcomes.

1. The plan is frozen before mutation, and apply revalidates the manifest, inputs, output generations, and tagged write history. The CLI flag is only an operator acknowledgement; exclusive write ownership still depends on deployment controls.
2. The current strategy rebuilds one full target per ordered step, tags its commits, and records the observed target/read versions. It does not offer arbitrary row/date chunking or a cross-table publication transaction.
3. Apply reconciles tagged commits against its ledger so a retry can account for completed steps. Validate this recovery path on the deployed Delta runtime, including driver failure between commit and ledger recording.
4. The rebuild runs in place. It does not stage the complete DAG for isolated validation before readers can observe each table's new version. Coordinate readers when a consistent multi-table view is required.
5. Rollback is a new recorded operation. The current implementation allows whole-table RESTORE only when the retained history, table generation, and intervening commits match the repair's recorded write set; it also rewinds source cursors. A compensating correction or fresh rebuild is needed outside that case.
6. Keep retention, target-write ownership, and downstream coordination in the production acceptance tests. Do not VACUUM data required for the planned restore while the repair is open.

For streaming, a table snapshot is insufficient: a plan needs compatible source offsets, operator state, and a defined restart boundary. If that capability is unavailable, use an explicitly scoped batch rebuild/cutover or mark the strategy unsupported. Include historical tables, identity maps, and other state that affects dimension/fact key consistency in the repair boundary.

Concrete counterexamples define what robustness must mean:

| Scenario | Required outcome |
|---|---|
| Bad patch at v42, good write at v43 | A request to undo v42 must preserve v43 or explicitly plan a broader coordinated rebuild; plain RESTORE v41 is rejected as selective undo. |
| Crash after data commit, before audit completion | Reconciliation finds the same operation token and records the actual outcome; no duplicate mutation. |
| Only intent exists; no matching target commit | Retry the same frozen logical operation under the ownership policy. Incomplete retained history yields unknown, not assumed absence. |
| Concurrent writer arrives between plan and execution | Revalidate and reject stale assumptions, or serialize writers using an enforceable mechanism. |
| Zero-row/no-op repair | Record successful validation and no target commit where appropriate; do not invent an incremented version. |
| VACUUM removed the pre-repair snapshot | Report unavailable rollback; use retained raw/archive data or a backup for an explicit rebuild. An integer version alone is insufficient. |
| Table dropped and recreated under the same name | Detect a different physical generation; do not apply a version number from the old table. |
| One source changes a join key or adds a formerly missing match | Recompute old and new affected key groups, including new/deleted outputs. Existing output lineage alone does not cover previously absent rows. |
| Correction changes an SCD2 effective date | Rebuild the necessary entity history and reassess fact key assignments. One source commit need not imply one bounded date partition. |
| Only one of multiple durable target writes completes | Expose partial publication and recover all managed participants; no aggregate SUCCESS from the first commit. |
| Late business correction is recorded today | Keep today's recorded time and the earlier effective interval; preserve both as-known-then and corrected-history interpretations if required. |
| Same repair is requested twice | Stable revision/plan/scope identity detects already completed work; an intentional reapplication creates an explicit new revision or plan. |

**Ease-of-use assessment [INFERRED].** The initial CLI accepts a bad source version, optional replacement version, reason, and optional explicit output targets; it computes configured downstream scope and persists a reportable normalized ledger. The plan/status expose reads, writes, commits, timing, and rollback points. The CLI does not accept an arbitrary row-level business scope or typed correction artifact, nor does it yet report all externally managed downstream products. There is no user study of this interface; measure operator actions, manual SQL edits, incorrect recoveries, and time to a correct plan against today's workflow before claiming it is easier.

For usability evaluation, test whether operators can identify affected tables without opening Delta logs, resume after a driver crash without editing metadata, explain why rollback is unavailable, and repair one source feeding multiple models without selecting every downstream target manually. Measure operator actions, manual SQL edits, incorrect recoveries, and time to a correct plan against today's workflow. Performance claims require representative Spark workloads and measured metadata-write costs.

**Next work [INFERRED].** First run the failure scenarios on the supported Databricks/Delta runtime and measure a representative repair. Then evaluate operator usability and close any discrepancies between the ledger and observed writes. Stateful SCD reconstruction, streaming replay, row/date-scoped repair, and atomic multi-table publication remain separate capabilities requiring explicit correctness contracts. Add fine-grained row provenance only if a demonstrated repair/audit need cannot be met by per-step reads and scoped recomputation. SQLMesh or lakeFS may provide isolation/publication primitives, but their fit with custom SCD and Databricks behavior still needs validation.

**Claims ledger.** The audit below distinguishes documented evidence from architectural inference.

| Claim | Tag | Evidence inspected | Check needed before implementation relies on it |
|---|---|---|---|
| Existing systems offer planned restatement, lifecycle metadata, and isolated validation patterns | VERIFIED | SQLMesh, OpenLineage, lakeFS primary documentation above | Pin selected integration versions and confirm deployed engine support. |
| The framework has mutable run cursors, separate crash recovery, and a first normalized repair ledger | VERIFIED | `watermark.py`, `recover.py`, `repair_ledger.py`, `repair_execution.py`, `repair.py` | Exercise the paths on the supported runtime and inspect deployed table permissions/retention. |
| The initial repair path pins source versions and records actual dependency reads, commits, and timings | VERIFIED | Repair plan/apply/status implementation and unit tests | Live Delta scenario with multiple inputs and a downstream dependency. |
| The current repair path is safe for production workloads | NOT ESTABLISHED | No live Spark/Delta repair experiment or production history was inspected | Run acceptance scenarios, including concurrent writes, crashes, retention, and rollback. |
| The repair CLI is easier for operators than today's workflow | INFERRED | The plan computes configured scope and reports versions/commits | Task-based usability comparison against current recovery. |
| Universal row-level version dictionaries are needed | NOT ESTABLISHED | No reviewed source establishes this requirement | Demonstrate a precision/use case unmet by scoped recomputation and run provenance before adding them. |

**Information boundary.** This work inspected current source and first-party documentation, plus research full text for FaDE and TensProv and the IcedTea abstract/artifact. It did not inspect a deployed Databricks workspace, production history, source retention, external consumers, or real garbage-data incidents. Unit tests exist and passed in the implementation run, but no live Spark/Delta repair experiment has been run. A source/documented mechanism or unit test is not proof this implementation is production-robust. Existing uncommitted source changes mean this assessment is tied to the inspected working tree as well as the base commit.

**Validation checklist.** Research/design assessment completion and later implementation proof are deliberately different:

- [x] Compare similar implementations using primary sources and identify limitations.
- [x] Evaluate recent provenance/replay research and its transfer limits.
- [x] Assess applicable Data Vault and data mesh methods without requiring a warehouse migration.
- [x] Reinspect the current repository and map reuse/gaps.
- [x] Challenge the design with concrete rollback, concurrency, crash, temporal, and denormalization cases.
- [x] Evaluate operator workflow and define observable usability criteria.
- [ ] Before production use: execute failure scenarios on the supported Spark/Delta runtime with independently specified data and cursor outcomes.
- [ ] Before production use: demonstrate reproducible reads, preservation of unrelated writes, managed side-effect recovery, retention behavior, and retry convergence.
- [ ] Before production use: measure operator effort and runtime overhead on representative repairs.

The evidence supported a normalized ledger and planner design, and the working tree now contains its first snapshot-rebuild implementation. Production robustness and ease of use remain unverified until the acceptance scenarios above are completed.
