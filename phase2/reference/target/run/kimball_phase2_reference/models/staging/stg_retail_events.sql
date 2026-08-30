create or replace view dbt_reference.stg_retail_events
  
  
  as
    select
  cast(event_seq as bigint) as event_seq,
  fixture_state,
  source_system,
  line_id,
  invoice_no,
  stock_code,
  description,
  cast(quantity as bigint) as quantity,
  cast(invoice_ts as timestamp) as invoice_ts,
  cast(unit_price as decimal(18, 2)) as unit_price,
  customer_id,
  country,
  customer_segment,
  event_type
from dbt_reference_source.retail_events
where event_seq <= 14
