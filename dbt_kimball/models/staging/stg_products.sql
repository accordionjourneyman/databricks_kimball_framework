-- Staging model for products

SELECT
    product_id,
    name,
    category,
    unit_cost,
    updated_at,
    current_timestamp() as _loaded_at
FROM {{ source('silver', 'products') }}
