-- SCD Type 1 Dimension for Products
-- Uses incremental materialization - updates in place, no history

{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='append_new_columns'
    )
}}

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

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
