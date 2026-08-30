{% snapshot customer_snapshot %}
{{
  config(
    target_schema=target.schema ~ '_snapshots',
    unique_key="concat(source_system, ':', customer_id)",
    strategy='check',
    file_format='delta',
    check_cols=['country', 'customer_segment']
  )
}}
select source_system, customer_id, country, customer_segment
from {{ ref('dim_customer') }}
where is_current
{% endsnapshot %}
