-- Staging model for products
-- Supports CDF (Change Data Feed) with timestamp fallback

{% set target_table = 'demo_gold.dim_product' %}
{% set source_table = 'demo_silver.products' %}
{# CRITICAL: Add 1 to avoid reprocessing the last version #}
{% set last_version = get_watermark(target_table, source_table, 'version') %}
{% set next_version = last_version ~ ' + 1' %}

{% if var('use_cdf', true) and is_incremental() %}
-- CDF Mode: Read from table_changes view
WITH source_cdf AS (
    SELECT 
        product_id,
        name,
        category,
        unit_cost,
        updated_at,
        _change_type,
        _commit_version,
        current_timestamp() as _loaded_at
    FROM table_changes('{{ source_table }}', {{ next_version }})
    WHERE _change_type != 'update_preimage'
)
{{ deduplicate_cdf('source_cdf', ['product_id']) }}

{% else %}
-- Full/Timestamp Mode
SELECT
    product_id,
    name,
    category,
    unit_cost,
    updated_at,
    NULL as _change_type,
    NULL as _commit_version,
    current_timestamp() as _loaded_at
FROM {{ source('silver', 'products') }}
{% endif %}

