-- SCD Type 2 Snapshot for Customer Dimension
-- Uses dbt's native snapshot functionality to track history

{% snapshot dim_customer %}

{{
    config(
        target_schema='demo_gold',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 'last_name', 'email', 'address'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    -- Surrogate key using hash of natural key
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
    
    -- Natural key
    customer_id,
    
    -- Attributes (tracked for history)
    first_name,
    last_name,
    email,
    address,
    
    -- Audit columns
    updated_at,
    current_timestamp() as __etl_processed_at

FROM {{ ref('stg_customers') }}

{% endsnapshot %}
