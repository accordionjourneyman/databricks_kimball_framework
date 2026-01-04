-- SCD Type 1 Dimension for Products
-- Uses incremental materialization with default rows

{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='append_new_columns'
    )
}}

-- Default Kimball rows (Unknown, Not Applicable, Error)
WITH default_rows AS (
    SELECT 
        CAST(product_sk AS STRING) as product_sk,
        CAST(product_id AS INT) as product_id,
        name,
        category,
        CAST(unit_cost AS DOUBLE) as unit_cost,
        CAST(updated_at AS TIMESTAMP) as updated_at,
        current_timestamp() as __etl_processed_at
    FROM {{ ref('default_dim_product') }}
),

-- Source product data
source_products AS (
    SELECT
        -- Surrogate key (identity-like, using row hash)
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
        
        -- Natural key
        product_id,
        
        -- Attributes (overwritten on update)
        name,
        category,
        unit_cost,
        
        -- Audit columns
        updated_at,
        current_timestamp() as __etl_processed_at

    FROM {{ ref('stg_products') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) = 1
)

-- On full refresh, include defaults; on incremental, just new source data
{% if not is_incremental() %}
SELECT * FROM default_rows
UNION ALL
{% endif %}
SELECT * FROM source_products

{% if is_incremental() %}
-- CDF provides filtered data
WHERE 1=1
{% endif %}
