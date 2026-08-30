{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='delta'
  )
}}

select id, value
from {{ ref('smoke_input') }}
where id <= {{ var('upper_bound', 2) }}
{% if is_incremental() %}
  and id > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
