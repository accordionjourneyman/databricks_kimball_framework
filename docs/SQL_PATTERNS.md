# Framework features versus SQL patterns

YAML can validate intent and automate repeatable mechanics, but it cannot infer
business events or design a dimensional model. The distinction below is part of
the framework's public scope.

| Pattern | Framework feature | User-owned SQL/modeling |
| --- | --- | --- |
| Transaction fact | Declares pattern/grain, merge keys, FKs, measures, additivity, and validates output columns. | Derive the event row and measures at the declared grain. |
| Periodic snapshot | Requires `snapshot_period`, grain, keys, and measure semantics. | Generate the snapshot calendar, opening/closing balances, and missing-period policy. |
| Accumulating snapshot | Requires ordered unique milestones and validates milestone columns. | Collapse events into one lifecycle row and define rework/reversal rules. |
| Role-playing dimension | Explicit FK role metadata; bus matrix collapses roles under one physical dimension. | Resolve each role key (`order_date_sk`, `ship_date_sk`) in transformation SQL. |
| Degenerate dimension | Declares and validates identifiers retained on the fact. | Select/normalize the business identifier; no dimension table is created. |
| Junk dimension | Materializes distinct flag combinations, deterministic SKs, collision checks, idempotent Delta merge, and fact FK validation. | Choose low-cardinality attributes that belong together and normalize their values. |
| SCD1/2/4/6 | Version/current/history mechanics, deletes, business-effective time, replay behavior, and system columns. | Define natural keys and business attributes; SCD4 uses field-level audit history rather than a Kimball mini-dimension. |
| Early-arriving fact | Creates/hydrates dimension skeletons for configured relationships. | Decide whether the business permits skeletons and how missing source attributes are later supplied. |
| Factless fact | No dedicated template is necessary; a fact with keys and no measures is valid. | Define the coverage/event grain and transformation SQL. |
| Many-to-many bridge | No first-class bridge lifecycle feature. | Build bridge rows, group keys, weighting/allocation rules, and effective dating in SQL. |
| Hierarchy/closure | No first-class hierarchy engine. | Build parent-child, flattened, path, or closure tables and cycle rules in SQL. |
| SCD0/SCD3 | Not implemented. | Use explicit SQL only with documented ownership; it is not managed by SCD dispatch. |

Checked-in examples in `examples/sql_patterns` are educational SQL templates.
They are not automatically invoked by `fact_pattern`. In particular, declaring
`periodic_snapshot` does not invent inventory balance logic, and declaring
`accumulating_snapshot` does not infer milestone timestamps from an event log.

## Output contract

For facts, the runtime validates that transformed output includes:

- Every merge key and declared foreign key.
- Every measure and milestone column.
- Every degenerate dimension.
- Every generated junk-dimension surrogate key after materialization.

Configuration validation checks the declaration itself; runtime validation
checks the DataFrame the SQL actually produced. This prevents documentation
metadata from drifting away from executable behavior.

## Enterprise bus matrix semantics

The bus matrix represents physical conformed dimensions. If a fact declares
two role-playing keys to `dim_date`, the output contains one `dim_date` column
with both roles, for example `X (order_date, ship_date)`. Degenerate dimensions
do not create bus columns. Managed junk dimensions do create physical dimension
columns. Conformance warnings compare declared canonical ownership and shared
attributes; they cannot establish enterprise business meaning by themselves.
