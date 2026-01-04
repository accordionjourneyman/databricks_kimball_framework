-- SCD Type 2 Snapshot for Customer Dimension
-- Uses dbt's native snapshot functionality to track history

{% snapshot dim_customer %}

{{
    config(
        target_schema='demo_gold',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 'last_name', 'email', 'address'],
        invalidate_hard_deletes=True,
        post_hook="
            MERGE INTO {{ this }} AS target
            USING (
                SELECT 
                    CAST(customer_sk AS STRING) as customer_sk,
                    CAST(customer_id AS INT) as customer_id,
                    first_name, last_name, email, address,
                    CAST(dbt_valid_from AS TIMESTAMP) as dbt_valid_from,
                    CAST(NULL AS TIMESTAMP) as dbt_valid_to,
                    CAST('1900-01-01' AS TIMESTAMP) as dbt_updated_at,
                    '{{ invocation_id }}' as dbt_scd_id,
                    CAST('1900-01-01' AS TIMESTAMP) as updated_at,
                    current_timestamp() as __etl_processed_at
                FROM {{ ref('default_dim_customer') }}
            ) AS defaults
            ON target.customer_sk = defaults.customer_sk
            WHEN MATCHED THEN UPDATE SET
                target.first_name = defaults.first_name,
                target.last_name = defaults.last_name,
                target.email = defaults.email,
                target.address = defaults.address
            WHEN NOT MATCHED THEN INSERT *
        "
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

-- Use source directly (not staging view) since snapshot runs before models
FROM {{ source('silver', 'customers') }}

{% endsnapshot %}
