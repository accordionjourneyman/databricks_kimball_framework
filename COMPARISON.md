# Comparison

`databricks_kimball_framework` (imported as `kimball`) is **not the first** library to do
Kimball dimensional modeling or slowly changing dimensions on Databricks — SCD is a
Kimball-era pattern (~1996) and the lakehouse/Delta substrate has a crowded tooling landscape.
This page is an honest map: who does which SCD types, who has the orchestrator/recovery
machinery this framework is built around, how rigorously each is tested, and where this
framework is — and isn't — distinctive. The goal is to pick the right tool and to avoid
over-claiming.

> Surveyed July 2026 by direct repository inspection (cloned locally) combined with the
> secondary landscape survey in [`data-hub-gandalf`'s COMPARISON.md](https://github.com/juntossomosmais/data-hub-gandalf/blob/main/COMPARISON.md)
> (June 2026, 53 candidates examined / 21 confirmed SCD libraries). Rows marked **(inspected)**
> were verified by reading source; rows marked **(gandalf survey)** inherit that project's
> published claims and should be re-verified before you depend on them.

## SCD type coverage

Legend: ✅ supported · `—` not supported · ⚠️ recognized but not implemented.

| Library | Ecosystem | 0 | 1 | 2 | 3 | 4 | 6 | Source |
|---|---|:-:|:-:|:-:|:-:|:-:|:-:|---|
| **kimball** (this) | PySpark + Delta, batch + streaming orchestrator | — | ✅ | ✅ | — | ✅ | ✅ | inspected |
| [data-hub-gandalf](https://github.com/juntossomosmais/data-hub-gandalf) | PySpark + Delta over Spark Connect | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | inspected (Apache-2.0) |
| [fabricks](https://github.com/fabricks-framework/fabricks) | Databricks + PySpark + Delta | ✅ | ✅ | ✅ | — | — | — | gandalf survey |
| [koheesio](https://github.com/Nike-Inc/koheesio) (Nike) | PySpark + Delta | — | ✅ | ✅ | — | — | — | gandalf survey |
| [hydro](https://github.com/christophergrant/hydro) | PySpark + Delta | — | ✅ | ✅ | — | — | — | gandalf survey |
| [mack](https://github.com/MrPowers/mack) | PySpark + Delta | — | — | ✅ | — | — | — | gandalf survey |
| [dbxscd](https://github.com/maye-msft/SlowlyChangingDimensionsInDeltaLake) | Databricks + Delta | — | ✅ | ✅ | ✅ | — | — | gandalf survey |
| [Azure SCD-Merge-Wizard](https://github.com/Azure-Player/SCD-Merge-Wizard) | T-SQL / SQL Server | ✅ | ✅ | ✅ | ✅ | — | — | gandalf survey |
| [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots) | dbt (multi-warehouse) | — | — | ✅ | — | — | — | inspected |
| [Databricks DLT — AUTO CDC / APPLY CHANGES](https://learn.microsoft.com/azure/databricks/ldp/cdc) | Databricks (first-party) | — | ✅ | ✅ | — | — | — | inspected |
| [daq-databricks-dab](https://github.com/yasarkocyigit/daq-databricks-dab) | DAB + Lakeflow Declarative Pipelines | — | ✅ | ✅ | — | — | — | inspected (no license) |
| [databricks-medallion](https://github.com/romaklimenko/databricks-medallion) | DAB demo (7 approaches) | — | ✅ | ✅ | — | — | — | inspected (no license) |
| [kimball_modelling_demo](https://github.com/sol-db/kimball_modelling_demo) | DLT + DAB demo | — | — | ✅ | — | — | — | inspected (no license) |

Notes:
- **kimball does not implement SCD0 or SCD3.** SCD4 (mini-dimension / history table) and SCD6
  (hybrid) in one library is rare — among the surveyed tools only `data-hub-gandalf` matches
  that coverage.
- **CDC SCD2 is the differentiator and the risk.** `gandalf` recognizes an `scd2_cdc` alias but
  intentionally raises `NotImplementedError` ("event-ordering / late-event semantics are not
  built"). `kimball` *does* attempt CDF-based incremental SCD2 with multi-version back-fill and
  out-of-order versions — see **Known correctness gaps** below. DLT `APPLY CHANGES` handles
  out-of-order natively via `sequence_by` and is the industrial reference for that case.

## Beyond-merge capabilities (the orchestrator layer)

SCD merge mechanics are table stakes. The dimensions below are where this framework is meant to
add value, and where the landscape thins out.

Legend: ✅ yes · `—` no · ⚠️ partial / has known gaps.

| Capability | kimball | gandalf | fabricks | koheesio | dbt snapshots | DLT APPLY CHANGES | daq-databricks-dab |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| CDF incremental loading + watermark tracking | ✅ | — | ⚠️ | ⚠️ | — | ✅ (managed) | ✅ (DLT-managed) |
| Out-of-order / late-event SCD2 | ⚠️ | — | — | — | — | ✅ | ✅ (DLT-managed) |
| Deterministic surrogate keys | ✅ hash + identity | ✅ dim + fact | — | ⚠️ | — | — | — |
| Batch orchestrator + `etl_control` audit | ✅ | — | ✅ | — | ✅ (via dbt) | ✅ (managed) | ✅ (control-state) |
| Crash recovery / zombie rollback (Delta RESTORE) | ⚠️ | — | — | — | — | ✅ (managed) | — |
| Early-arriving-fact skeletons + late-dim hydration | ⚠️ | — | — | — | — | — | — |
| Structured-streaming path (`foreachBatch`) | ⚠️ | — | — | — | — | ✅ (native) | ✅ (DLT) |
| Schema evolution inside merge | ✅ | ✅ | ✅ | ⚠️ | ✅ | ✅ | ✅ |
| Soft / hard delete strategies | ✅ | ✅ | ⚠️ | ⚠️ | ✅ | ✅ | ⚠️ |
| Declarative YAML config | ✅ | ⚠️ (kwargs) | ✅ | ✅ | ✅ | ✅ (SQL/Python) | ✅ |

What the ⚠️ marks on `kimball` mean (all detailed in [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md)
and the July 2026 correctness review):

- **Out-of-order / multi-version SCD2 — `_merge_single_pass`:** the multi-version back-fill path
  (chosen when ≥2 versions of a key arrive in one batch) has two critical bugs — skeleton rows
  are not hydrated (a null-SK duplicate is inserted instead), and the expire row's `__valid_to`
  is set to the *latest* new version's `valid_from`, creating validity-chain overlaps that corrupt
  point-in-time reads. The single-version `_merge_classic` path is correct. Fix in progress.
- **Crash recovery / zombie rollback:** implemented for the batch path via Delta time-travel
  `RESTORE`, but `recover_zombies` only scans `.history(10)` (incomplete for >10-commit batches),
  and the streaming path has no transaction wrapper / zombie recovery at all.
- **Early-arriving-fact skeletons:** implemented and correct in the batch path; **non-functional
  in streaming** (the streaming `foreachBatch` does not call `generate_skeletons`).
- **Structured-streaming path:** first-class but missing several batch safety nets — no
  `table_transaction`/RESTORE, no `recover_zombies`, no `batch_id` commit tagging, no adaptive
  pruning. See `KNOWN_LIMITATIONS.md` §8.

## Test rigor

Does the project ship automated tests that execute the SCD merge against a real engine, and on
what?

| Library | Real-engine E2E? | Engine in tests / CI | On real Databricks? | Source |
|---|:-:|---|:-:|---|
| **kimball** (this) | ✅ | local Spark + Delta; golden-file + 9 integration suites; 10 benchmark suites | ⚠️ integration workflow exists, not default CI | inspected |
| data-hub-gandalf | ✅ | Spark Connect + Delta (Dockerized, in CI); databricks-connect opt-in | ⚠️ opt-in, not default CI | inspected |
| fabricks | ✅ | Spark Connect + real Databricks cluster (Asset Bundle job) | ✅ | gandalf survey |
| mack / koheesio / hydro | ✅ | local Spark (+ Delta) | — | gandalf survey |
| dbt snapshots (dbt-databricks) | ✅ | real Databricks (functional tests) | ✅ | inspected |
| DLT APPLY CHANGES | ✅ | real Databricks (managed) | ✅ | inspected |
| daq-databricks-dab / databricks-medallion / kimball_modelling_demo | — | DAB/DLT demos, no standalone SCD test suite | — | inspected |

`kimball`'s test surface (per the CI badge): 97 unit + 22 integration + 10 benchmark tests,
with golden-file fixtures (`tests/golden/`) and integration suites over public datasets
(NYC Taxi, Olist, Online Retail, Synthea). The gaps vs. the most rigorous peers: Databricks is
not exercised in default CI, and the multi-version / out-of-order SCD2 scenarios (where the
critical bugs live) are the least covered by golden files.

## Where kimball is — and isn't — distinctive

**Distinctive (uncommon combination):**

- **CDF-based incremental SCD2 with multi-version back-fill** inside a batch orchestrator — the
  one capability `gandalf` explicitly punts on. No surveyed library combines this with SCD4/6.
- **Crash recovery via Delta time-travel `RESTORE` + zombie-batch detection** as a first-class
  orchestrator feature (not just "MERGE is idempotent, re-run it"). DLT does this managed;
  among *self-authored* libraries this is uncommon.
- **Early-arriving-fact skeletons + late-arriving-dimension hydration** in the batch path — not
  found in any other surveyed tool.
- **SCD 1/2/4/6 + hash *and* identity surrogate keys + soft/hard deletes + schema evolution**
  in one declarative YAML config.

**Not distinctive — honest caveats:**

- **Not the first SCD library.** SQL Server (2012), dbt snapshots (2019), dbxscd (2021),
  mack & DLT (2022), koheesio/fabricks/gandalf (2024–2026) all predate it.
- **Not the most complete SCD range.** `gandalf` covers 0/1/2/3/4/6; `kimball` covers 1/2/4/6
  (no SCD0 or SCD3).
- **Not the only framework tested on real Databricks.** `fabricks` and `dbt-databricks` run SCD
  tests against a real Databricks cluster; `kimball`'s Databricks path is an opt-in workflow.
- **Out-of-order SCD2 is attempted but not yet correct** — DLT `APPLY CHANGES` remains the
  reference implementation for that case today.
- **Single-writer assumption per target table** (see `KNOWN_LIMITATIONS.md` §3).

## When to use what (quick guide)

- **Sub-minute freshness / streaming / out-of-order CDC on Databricks** → DLT `APPLY CHANGES`
  (managed, correct out-of-order) or `kimball`'s streaming path (once the parity gaps close).
- **Batch dimensional modeling on Databricks with crash recovery + watermarks + skeletons** →
  `kimball` (this framework).
- **Just need SCD1/2/3/4/6 merge mechanics, pip-installable, no orchestrator** →
  `data-hub-gandalf` (most mature SCD range, Apache-2.0).
- **dbt-centric stack, batch-only, no out-of-order** → `dbt snapshots`.
- **Full medallion DAB + Lakeflow Declarative Pipelines** → `daq-databricks-dab` (no license —
  read-only reference).

## Relation to the scientific literature

This page is a **tooling landscape** survey. Design decisions are additionally grounded in the
academic literature review kept alongside this repo (Kimball & Ross 2013; Golfarelli & Rizzi on
temporal DW; Armbrust et al. 2021 and Schneider et al. 2024 on lakehouse requirements; Jain et
al. 2023 on Delta CoW/MoR and transaction isolation; Whittles 2012 on SCD2 load performance).
Closest published analogues to that review:

- Lanzas Martínez (2024), *"Slowly changing dimensions in data warehouses: an analysis through
  literature review"*, Revista Científica Estelí 13(49) — [DOI 10.53777/esteli.v13i49.17890](https://doi.org/10.53777/esteli.v13i49.17890).
- Schneider et al. (2024), *"The Lakehouse: State of the Art on Concepts and Technologies"*,
  SN Computer Science 5:449 — [DOI 10.1007/s42979-024-02737-0](https://link.springer.com/article/10.1007/s42979-024-02737-0).
- Jain et al. (2023), *"Analyzing and Comparing Lakehouse Storage Systems"*, CIDR 2023 — [PDF](https://www.vldb.org/cidrdb/papers/2023/p92-jain.pdf).

## Methodology & caveats

- Direct inspection (July 2026): `data-hub-gandalf`, `daq-databricks-dab`, `databricks-medallion`,
  `kimball_modelling_demo`, plus dbt snapshots and DLT `APPLY CHANGES` docs. Repos cloned to a
  local comparisons directory for source reading.
- Secondary: SCD-type and test-rigor rows for `mack`, `koheesio`, `hydro`, `fabricks`, `dbxscd`,
  `Azure SCD-Merge-Wizard`, `levi`, `superlake`, `spark-fuse`, `odbc2deltalake` are inherited from
  `data-hub-gandalf`'s June 2026 `COMPARISON.md` and were not independently re-verified here.
- **"First of its kind" is not a claim this project makes.** Proving absence across the
  GitHub/PyPI long tail is not possible; this is a best-effort survey, not a proof.
- **Licenses matter for reuse, not just reading.** `data-hub-gandalf` is Apache-2.0 (code may be
  reused with attribution). `daq-databricks-dab`, `databricks-medallion`, and
  `kimball_modelling_demo` ship **no LICENSE file** — default copyright reserves all rights, so
  they are read-only references; do not copy code from them.

## Sources

| Project | Link | License |
|---|---|---|
| data-hub-gandalf | https://github.com/juntossomosmais/data-hub-gandalf | Apache-2.0 |
| fabricks | https://github.com/fabricks-framework/fabricks | — |
| koheesio (Nike) | https://github.com/Nike-Inc/koheesio | — |
| hydro | https://github.com/christophergrant/hydro | — |
| mack | https://github.com/MrPowers/mack | — |
| dbxscd | https://github.com/maye-msft/SlowlyChangingDimensionsInDeltaLake | — |
| Azure SCD-Merge-Wizard | https://github.com/Azure-Player/SCD-Merge-Wizard | — |
| levi | https://github.com/mrpowers-io/levi | — |
| superlake | https://github.com/loicmagnien/superlake | — |
| spark-fuse | https://github.com/kevinsames/spark-fuse | — |
| odbc2deltalake | https://github.com/bmsuisse/odbc2deltalake | — |
| dbt snapshots / dbt-databricks | https://docs.getdbt.com/docs/build/snapshots · https://github.com/dbt-labs/dbt-databricks | Apache-2.0 |
| Databricks DLT — AUTO CDC / APPLY CHANGES | https://learn.microsoft.com/azure/databricks/ldp/cdc | proprietary (Databricks) |
| daq-databricks-dab | https://github.com/yasarkocyigit/daq-databricks-dab | none (all rights reserved) |
| databricks-medallion | https://github.com/romaklimenko/databricks-medallion | none (all rights reserved) |
| kimball_modelling_demo | https://github.com/sol-db/kimball_modelling_demo | none (all rights reserved) |