{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['source_system', 'stock_code']
  )
}}

with ranked as (
  select *,
    row_number() over (
      partition by source_system, stock_code order by event_seq desc
    ) as rn
  from {{ ref('stg_retail_events') }}
  where stock_code is not null and stock_code <> 'MISSING'
)
select
  source_system,
  stock_code,
  description,
  unit_price,
  event_seq as last_event_seq
from ranked
where rn = 1
