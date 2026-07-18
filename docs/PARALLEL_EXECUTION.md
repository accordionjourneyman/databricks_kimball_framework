# Dependency and parallel execution

The framework uses a real table dependency DAG. It does not divide a project
into only “all dimensions, then all facts”: a dimension may depend on another
dimension, facts may depend on selected upstreams, and auxiliary tables can
create writer conflicts.

## Declare and compile dependencies

```yaml
table_name: gold.fact_sales
table_type: fact
depends_on: [gold.dim_customer, gold.dim_product]
sources:
  - name: gold.dim_customer
    alias: customer
    cdc_strategy: full
  - name: gold.dim_product
    alias: product
    cdc_strategy: full
```

```bash
kimball validate --config configs --target prod
kimball compile --config configs --target prod --output manifest.json
kimball plan --config configs --target prod --against deployed-manifest.json
kimball compile --config configs --target prod --bundle-output generated-job.yml
```

Production validation rejects:

- Cycles and missing declared upstreams.
- Inferred in-project dependencies omitted from `depends_on`.
- Multiple configurations writing one target.
- Conflicts on managed history or junk-dimension tables.

Downstream nodes of a failed pipeline are skipped selectively; unrelated DAG
branches remain eligible.

## In-process execution

`PipelineExecutor` is deterministic and serial even if legacy callers pass
`max_workers`. One Python process and one Spark session are not a safe
isolation boundary for concurrent DDL/DML, temp views, or compensating
rollback.

```python
from kimball import PipelineExecutor

summary = PipelineExecutor(
    config_paths=[
        "configs/dim_customer.yml",
        "configs/dim_product.yml",
        "configs/fact_sales.yml",
    ],
    etl_schema="gold_ops",
    profile="production",
    stop_on_failure=True,
).run()
```

Use this path for notebooks, development, and a single serial job task.

## Production parallelism

Use the generated Databricks Asset Bundle. Each pipeline is a distinct job task,
dependencies come from the compiled DAG, and independent ready tasks may run in
parallel on isolated task execution contexts. The generated job sets
`max_concurrent_runs: 1` because recovery and watermark correctness require a
single active project run and one writer per target.

Do not add Python `ThreadPoolExecutor` wrappers around orchestrators or Spark
ingestion helpers. If an external scheduler is used instead of the generated
bundle, it must reproduce:

1. The compiled dependency edges.
2. One active writer for every target and auxiliary table.
3. Selective downstream skip after upstream failure.
4. Retry policy that does not overlap a previous active run.
5. Retained deployment manifest for plan/diff and rollback review.

## Monitoring

Databricks Jobs supplies task state, retries, duration, and alerts. Framework
tables supply domain evidence: `etl_control`, `etl_data_quality_events`, and
`etl_contract_temporal_state`. Use both layers; job success alone does not
prove freshness or data quality.
