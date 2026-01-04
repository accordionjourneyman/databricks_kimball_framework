{% snapshot dim_customer %}

{{
    config(
        target_schema='demo_gold',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

WITH source_data AS (
    SELECT 
        -- Surrogate key using hash of natural key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
        customer_id,
        first_name,
        last_name,
        email,
        address,
        CAST(updated_at AS TIMESTAMP) as updated_at,
        current_timestamp() as __etl_processed_at
    FROM {{ source('silver', 'customers') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
),

defaults AS (
    SELECT 
        CAST(customer_sk AS STRING) as customer_sk,
        customer_id,
        first_name,
        last_name,
        email,
        address,
        CAST('1900-01-01' AS TIMESTAMP) as updated_at,
        current_timestamp() as __etl_processed_at
    FROM {{ ref('default_dim_customer') }}
)

SELECT * FROM source_data
UNION ALL
SELECT * FROM defaults

{% endsnapshot %}
