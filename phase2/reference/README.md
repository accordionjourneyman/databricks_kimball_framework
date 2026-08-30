# Controlled Kimball reference

This project compares native dbt behavior with an independent PySpark
implementation against the same immutable fixture and ordered mutation ledger.
It is an assessment fixture, not a production retail model.

## Target contract

- `dim_product`: SCD1 at `(source_system, stock_code)`.
- `dim_customer`: SCD2 at
  `(source_system, customer_id, valid_from)`, half-open intervals, one current
  row, and no temporal overlap.
- `fact_sales`: transaction fact at `(source_system, line_id)`. Replays merge
  deterministically by latest `event_seq`; facts retain signed quantity and
  amount and resolve customers as `MATCHED`, `UNKNOWN`, or `SKELETON`.
- `identity_crosswalk`: explicit mapping from source-scoped customer identity to
  enterprise identity. Matching text alone never merges identities.

The dbt side deliberately uses sources, `ref`, view and incremental
materializations, generic tests, generated artifacts, and a native check
snapshot. The PySpark implementation does not import or execute compiled dbt
SQL. Both use Spark 4.0.1, Delta 4.2.0, the same PostgreSQL-backed Hive
metastore, isolated schemas, and the same fixture.

Run the complete local comparison from the repository root:

```bash
sg docker -c './phase2/harness/run-reference.sh'
```

The runner resets only its four Phase 2 databases, applies every state in
order, runs dbt models/snapshot/tests, independently builds the framework
tables, and writes a reconciliation JSON file per state under `evidence/`.

The input contract and derivation record are in
`../evidence/2.1-source-data-dossier.md`. Generated `target/` and `logs/`
directories are retained as dbt evidence, not treated as authored source.

