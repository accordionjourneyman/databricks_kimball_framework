# Literature review

This document maps the framework's design decisions to the scientific and
engineering literature they rest on, and — honestly — flags the decisions that
are *not* backed by a published precedent (the gaps where this project is
extending rather than applying known results). It is the reconstruction of a
literature review that supported the original design; the tooling landscape
that complements it lives in [`../COMPARISON.md`](../COMPARISON.md).

> Reconstructed July 2026. Citations are to the versions located during the
> review; where a paper is preprint / conference, the venue and year are given.
> Where a decision is **not** found in the literature, that is stated
> explicitly under "Gaps" rather than papered over.

## References (primary)

| # | Citation | Relevance |
|---|---|---|
| K&R | Kimball, R. & Ross, M. (2013). *The Data Warehouse Toolkit*, 3rd ed. Wiley. | Dimensional modeling, conformed dimensions, SCD types, surrogate keys, late-arriving facts. |
| GR | Golfarelli, M. & Rizzi, S. (various; see *Data Warehouse Design: Modern Principles and Methods*, McGraw-Hill, 2009, and journal papers on temporal DW). | Temporal data warehousing, slowly-changing & bi-temporal modeling, validity intervals. |
| Armbrust | Armbrust, K. et al. (2021). *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*. CIDR 2021. | The lakehouse substrate (Delta transactions, ACID, schema enforcement) this framework targets. |
| Schneider | Schneider, J. et al. (2024). *The Lakehouse: State of the Art on Concepts and Technologies*. SN Computer Science 5:449. [DOI 10.1007/s42979-024-02737-0](https://link.springer.com/article/10.1007/s42979-024-02737-0). | Consolidates lakehouse requirements (R1–R8) that motivate the orchestrator layer. |
| Jain | Jain, A. et al. (2023). *Analyzing and Comparing Lakehouse Storage Systems*. CIDR 2023. [PDF](https://www.vldb.org/cidrdb/papers/2023/p92-jain.pdf). | Copy-on-Write vs Merge-on-Read, transaction isolation across Delta/Hudi/Iceberg. |
| Whittles | Whittles, J. (2012). *Extracting Value from Chaos* / SCD2 load-performance discussion (SQL Server lineage). | The MERGE-vs-insert/expire performance motivation for single-pass SCD2. |
| Lanzas | Lanzas Martínez, A. (2024). *Slowly changing dimensions in data warehouses: an analysis through literature review*. Revista Científica Estelí 13(49). [DOI 10.53777/esteli.v13i49.17890](https://doi.org/10.53777/esteli.v13i49.17890). | The closest published SCD literature review; anchors the SCD taxonomy and confirms no canonical solution to out-of-order / CDC SCD2. |

## Decision → evidence map

### Dimensional modeling: star schemas, conformed dimensions, surrogate keys
- **Decision.** Dimensions and facts as physically separate Delta tables; natural
  keys tracked on dimensions; integer surrogate keys on every dimension and fact.
- **Evidence.** K&R (foundational); GR for the temporal extension. This is the
  uncontested core of dimensional modeling — no novelty claimed.

### SCD Type taxonomy (1/2/4/6; no SCD0/SCD3)
- **Decision.** Implement SCD1 (overwrite), SCD2 (history via validity interval),
  SCD4 (mini-dimension / history table) and SCD6 (hybrid). Omit SCD0 (fixed) and
  SCD3 (single historical snapshot).
- **Evidence.** K&R define SCD1/2/3; SCD0/4/6 are later extensions surveyed in
  Lanzas (2024) and GR. Lanzas confirms the taxonomy fragmentation — no single
  standard enumerates all of SCD0–6, which is why library coverage varies (see
  `COMPARISON.md`).

### SCD2 validity interval (`__valid_from` / `__valid_to` / `__is_current`, `− 1 MICROSECOND` upper bound)
- **Decision.** Half-open validity chain: each version is valid in
  `[valid_from, valid_to]` where `valid_to` of version *n* is
  `valid_from(n+1) − 1 MICROSECOND`; the current version has `valid_to = NULL`.
- **Evidence.** Standard bi-temporal / SCD2 interval convention from GR and the
  SCD2 lineage surveyed in Lanzas. The microsecond granularity matches Delta's
  `TIMESTAMP` precision and the convention used by dbt snapshots and DLT
  `APPLY CHANGES`.

### Deterministic surrogate keys via `xxhash64(natural_keys [+ version_col])`
- **Decision.** Surrogate keys are a deterministic hash so that re-runs and
  back-fills produce stable keys; the version column is folded into the hash for
  SCD2 so each version gets a distinct, idempotent key.
- **Evidence.** K&R prescribes integer surrogate keys but leaves generation open;
  content-addressable hashing is an engineering choice (cf. delta-rs / Hudi
  record keys). The *deterministic + version-folded* combination is not, to our
  knowledge, singled out in the academic literature — it is an implementation
  decision grounded in the idempotency requirement, not a cited result.

### CDF-based incremental loading with watermark tracking
- **Decision.** Use Delta's Change Data Feed (`_change_type`, `_commit_version`)
  as the incremental source, and an `etl_control` table to track per-source
  watermarks so batches are exactly-once and resumable.
- **Evidence.** Delta CDF is specified in the lakehouse literature — Armbrust
  (2021) and Schneider (2024) R-requirements cover ACID + change data. The
  *watermark/audit table* pattern is a classical DW control-table pattern (K&R
  ETL orchestration chapter) ported to the lakehouse.

### Multi-version SCD2 back-fill in a single MERGE (`_merge_single_pass`)
- **Decision.** When a batch contains ≥2 versions of a key, stage the full
  history chain (expire the old current row + insert all new versions) and apply
  it in **one** MERGE keyed on the surrogate key, instead of the classic
  single-pass SCD2 staging approach.
- **Evidence.** Whittles (2012) and the broader SCD2-load-performance lineage
  motivate reducing MERGE passes for throughput. The single-pass,
  SK-matched formulation is an engineering contribution of this project; Lanzas
  (2024) surveys SCD2 implementations and notes the absence of a canonical
  multi-version / out-of-order CDC solution, which is the gap this path targets
  (and where the two corrected bugs lived — see `COMPARISON.md` §"Known
  correctness gaps").

### Out-of-order / late-arriving versions
- **Decision.** Attempt to handle versions arriving out of business-time order
  by ranking on `_commit_version`/`_commit_timestamp`/`effective_at` and
  recomputing the validity chain from the staged set.
- **Evidence.** The *problem* is well known (GR temporal DW; Lanzas). The
  *reference solution* is DLT `APPLY CHANGES` with `sequence_by`, which is
  industrial rather than academic. This framework's batch attempt is partial —
  see "Gaps" below and `KNOWN_LIMITATIONS.md`.

### Early-arriving-fact skeletons + late-arriving-dimension hydration
- **Decision.** When a fact arrives before its dimension, insert a *skeleton*
  dimension row (placeholder attributes, a real surrogate key, `__is_skeleton =
  true`); when the dimension later arrives, **hydrate the skeleton in place** so
  the surrogate key is preserved and fact FKs that already point at it resolve.
- **Evidence.** K&R describes *late-arriving dimensions* and the "placeholder /
  unknown member" row, and recommends surrogate-key stability so fact FKs
  remain valid. The **skeleton + in-place hydration with a preserved SK**
  formulation is, to our knowledge, **not described in the surveyed literature**
  — see "Gaps".

### Crash recovery via Delta time-travel `RESTORE` + zombie-batch detection
- **Decision.** Wrap each target-table write in a transaction; on restart,
  detect partial ("zombie") batches by scanning `etl_control` and roll the
  target back to its pre-batch version with `RESTORE TABLE <t> VERSION AS OF`.
- **Evidence.** Delta's time travel and transactional guarantees are the
  lakehouse foundation (Armbrust 2021; Jain 2023 on isolation). The
  **zombie-detection + `RESTORE` rollback as an orchestrator-level recovery
  primitive** is an engineering contribution; managed runtimes (DLT) do this
  invisibly, but among self-authored libraries it is uncommon (see
  `COMPARISON.md`). Jain (2023) is the relevant citation for *why* this matters
  (CoW isolation makes per-batch restore safe).

### Structured-streaming `foreachBatch` path
- **Decision.** Expose a streaming entry point that reuses the batch merge
  inside `foreachBatch` for near-real-time SCD2.
- **Evidence.** Standard Spark Structured Streaming + `foreachBatch` pattern;
  Schneider (2024) lists streaming/low-latency ingestion as a lakehouse
  requirement. **Parity gaps vs the batch path remain** (no transaction wrapper
  / `RESTORE` / zombie recovery / skeletons in streaming) — see
  `KNOWN_LIMITATIONS.md` §8.

## Gaps — decisions not found in the surveyed literature

These are the parts of the design that extend beyond what the located papers
cover. They are flagged so they are not mistaken for established practice.

1. **Early-arriving-fact skeletons + late-arriving-dimension hydration (in-place,
   SK-preserving).** K&R gives the late-arriving-dimension *problem* and the
   unknown-member *pattern*, but not the skeleton-with-real-SK +
   in-place-hydration *mechanism* implemented here. No surveyed paper or library
   (see `COMPARISON.md`) describes it. It is a project contribution and
   therefore also a project risk — its correctness rests on the regression tests
   in `tests/integration/test_scd2_single_pass_regression.py`, not on a cited
   precedent.

2. **CDF-based incremental SCD2 with multi-version back-fill, combined with SCD4/6
   and crash recovery, in one orchestrator.** Lanzas (2024) explicitly notes the
   absence of a canonical solution to CDC/out-of-order SCD2; DLT `APPLY CHANGES`
   is the industrial reference but is proprietary and not SCD4/6. The
   *combination* in a self-authored library is, per `COMPARISON.md`, not found
   elsewhere — and the multi-version path is where the two corrected bugs lived,
   which is consistent with it being novel and under-explored.

3. **Zombie-batch detection + Delta `RESTORE` rollback as a first-class batch
   recovery primitive.** The substrate (time travel, CoW isolation — Armbrust,
   Jain) is published; the *orchestrator-level* recovery protocol built on it is
   an engineering design not described in the surveyed papers.

## How this review relates to the tooling survey

`COMPARISON.md` answers *"who else builds this, and how rigorously?"* against
the public library/tool landscape. This document answers *"what is the
published knowledge that justifies the design?"* The two overlap at the SCD
taxonomy (Lanzas 2024) and lakehouse substrate (Armbrust/Schneider/Jain), and
diverge exactly at the three gaps above — the places where this project is
doing something the literature and the landscape do not yet cover, and which
therefore carry the most correctness risk and test burden.