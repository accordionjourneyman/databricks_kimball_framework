with ranked_changes as (
  select
    source_system,
    customer_id,
    country,
    customer_segment,
    invoice_ts as valid_from,
    event_seq,
    row_number() over (
      partition by source_system, customer_id, invoice_ts order by event_seq desc
    ) as same_time_rank
  from dbt_reference.stg_retail_events
  where customer_id is not null and country is not null
),
deduplicated as (
  select * from ranked_changes where same_time_rank = 1
),
changes as (
  select *,
    lead(valid_from) over (
      partition by source_system, customer_id order by valid_from, event_seq
    ) as valid_to
  from deduplicated
)
select
  source_system,
  customer_id,
  country,
  customer_segment,
  valid_from,
  coalesce(valid_to, cast('9999-12-31 23:59:59' as timestamp)) as valid_to,
  valid_to is null as is_current
from changes