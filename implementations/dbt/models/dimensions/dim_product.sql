-- SCD Type 1 Dimension for Products
-- Uses incremental materialization with watermark tracking

{% set target_table = 'demo_gold.dim_product' %}
{% set source_table = 'demo_silver.products' %}

{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='append_new_columns',
        pre_hook="{{ batch_start(target_table, source_table) }}",
        post_hook="{{ batch_complete(target_table, source_table, new_version=get_latest_version(source_table)) }}"
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
-- CDF provides filtered data; for fallback use watermark
WHERE 1=1  -- CDF already filtered in staging
{% endif %}

