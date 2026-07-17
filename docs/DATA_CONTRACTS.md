# Data supplier contracts

The framework uses two complementary controls:

1. A versioned producer/consumer contract gate before deployment.
2. Runtime contract and DQ evidence for conditions that CI cannot observe.

Runtime monitoring alone discovers breakage after delivery. Schema comparison
alone cannot detect freshness, null-rate, duplicates, or late events. Production
use requires both.

## Canonical format and pinning

Producer contracts use Open Data Contract Standard (ODCS) `v3.1.0`. The exact
JSON Schema is vendored in the wheel, so validation does not depend on a moving
network resource. Contracts use semantic versions and a deterministic path:

```text
contracts/<contract-id>/<x.y.z>.odcs.yaml
```

A consumer pins an exact document:

```yaml
sources:
  - name: prod_silver.customers
    alias: customers
    cdc_strategy: cdf
    primary_keys: [customer_id]
    contract_ref: ../../contracts/customer-contract/1.0.0.odcs.yaml
```

The file path, ODCS `id`, and ODCS `version` must agree. The source table's
short name selects the matching ODCS schema object. Inline legacy contracts
remain supported, but `contract` and `contract_ref` are mutually exclusive.

## Producer CI gate

The repository workflow compares the proposed contract directory with the base
revision:

```bash
python tools/check_contract_changes.py \
  --previous base-contracts \
  --current contracts \
  --config configs
```

Compatibility rules are consumer-oriented:

| Change | Result |
| --- | --- |
| Add optional property | Compatible under `nullable_additions`. |
| Add required property | Breaking. |
| Remove property | Breaking. |
| Change physical/logical type | Breaking. |
| Make an optional property required | Breaking. |
| Change primary-key membership/order | Breaking. |
| Mutate an already-published version | Rejected by digest comparison. |
| Introduce breaking change without a major version | Rejected. |

The gate also resolves every consumer pin. Missing files, invalid ODCS,
contract/object mismatches, and stale pins fail before tables are touched.
Accepted versions may be registered in `etl_contract_registry` with the
canonical digest for audit.

## Runtime checks

The ODCS adapter drives the same consumer checks as an inline contract:

- Required columns, physical types, and nullability.
- CDF capability and primary-key agreement.
- Freshness.
- Compatible DQ rules such as not-null and uniqueness.
- Per-batch and cross-batch temporal ordering where configured.

Example runtime policy:

```yaml
contract:
  id: orders
  version: 2.1.0
  owner: order-platform
  schema:
    order_id: {type: bigint, nullable: false}
    event_at: {type: timestamp, nullable: false}
  cdc:
    required: true
    primary_key: [order_id]
  quality:
    - rule: not_null
      column: order_id
      severity: error
    - rule: null_rate
      column: event_at
      max_ratio: 0.001
      severity: error
  temporal:
    event_time_column: event_at
    allowed_lateness: 2 hours
    late_event_severity: warn
    out_of_order_severity: error
  validation:
    mode: full
    max_failure_samples: 5
    max_actions: 8
```

Temporal checks distinguish:

- Late arrival relative to the current evaluation time and allowed lateness.
- Out-of-order events inside a CDF batch using commit-version order.
- Events older than the persisted maximum for the same business key.

For facts with configured early-arriving-dimension handling, the framework also
records missing dimension keys as `early_arriving_fact` findings. A `skeleton`
policy creates unknown-member dimension rows; an `error` policy records the
same evidence and rejects the fact load.

The last category detects late/out-of-order data across job runs. State is
advanced only after a successful merge/watermark, preventing a failed batch
from teaching the monitor that invalid data was accepted.

## Operational monitoring

`ContractMonitor` performs read-only supplier checks without advancing
watermarks. Schedule it more frequently than the consuming ETL when early
warning matters. Findings go to `etl_data_quality_events`; error findings can
also be sent to a generic webhook through `KIMBALL_ALERT_WEBHOOK_URL`.

Alert on at least:

- Missing/incompatible required schema.
- CDF disabled or primary keys changed.
- Freshness SLO breach.
- Error-severity DQ or temporal finding.
- Failure to persist observability when `write_failure: error` is selected.

The event table is authoritative; webhook delivery is best effort. Create
Databricks SQL alerts or your incident integration over the table for durable
deduplication, ownership routing, and acknowledgement workflows.

## Supplier operating model

Ownership remains organizational, not merely technical. A useful contract
review records the producer, consumers, business definition, grain, service
levels, compatibility mode, deprecation window, and migration owner. A major
version is not complete until affected consumer manifests/plans have been
reviewed and downstream tables have a backfill or dual-read strategy.
