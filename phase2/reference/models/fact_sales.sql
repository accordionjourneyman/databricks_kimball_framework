{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['source_system', 'line_id']
  )
}}

with ranked_events as (
  select e.*,
    row_number() over (
      partition by source_system, line_id order by event_seq desc
    ) as replay_rank
  from {{ ref('stg_retail_events') }} e
  where e.event_type in ('sale', 'cancellation')
)
select
  e.source_system,
  e.line_id,
  e.invoice_no,
  e.stock_code,
  e.customer_id,
  e.invoice_ts,
  e.quantity,
  e.unit_price,
  cast(e.quantity * e.unit_price as decimal(18, 2)) as line_amount,
  e.event_type = 'cancellation' as is_cancellation,
  case
    when e.customer_id is null then 'UNKNOWN'
    when c.customer_id is null then 'SKELETON'
    else 'MATCHED'
  end as customer_resolution
from ranked_events e
left join {{ ref('dim_customer') }} c
  on e.source_system = c.source_system
 and e.customer_id = c.customer_id
 and e.invoice_ts >= c.valid_from
 and e.invoice_ts < c.valid_to
where e.replay_rank = 1
