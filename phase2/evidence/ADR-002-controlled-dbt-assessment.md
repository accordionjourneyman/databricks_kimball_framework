# ADR-002: retain a controlled dbt reference, not a parity target

Status: accepted  
Date: 2026-07-24

## Context

Phase 2 implemented the same source-state sequence in a native dbt project and
an independent PySpark reference. The pinned local Thrift architecture is
qualified and the implementations reconcile at the declared target columns.
dbt supplies an especially cohesive SQL development loop, graph, artifacts,
tests and native incremental/snapshot materializations. The framework retains
advantages in explicit execution control and the recovery/operator behavior
already proven in Phase 1.8d.

## Decision

Keep dbt as a maintained capability reference and regression oracle. Do not
make dbt project compatibility or migration a framework objective. Prioritize
machine-readable lineage and declarative DQ improvements that are independently
valuable, while retaining framework-specific recovery and PySpark execution
semantics.

Phase 2.6 subsequently passed on Databricks Free Edition using a native dbt
Lakeflow task and a dependent Serverless PySpark task. All included target rows
and types remained equivalent, so the conformance evidence does not change this
decision.

## Consequences

- The dbt reference remains small, pinned, reproducible and evidence-oriented.
- Equivalent outputs do not imply equivalent operational semantics.
- dbt-only conveniences such as snapshots need not be cloned without a concrete
  framework use case.
- Databricks SQL/Photon/Unity Catalog/CDF findings may change backlog priority,
  but cannot silently rewrite the local target contract.
