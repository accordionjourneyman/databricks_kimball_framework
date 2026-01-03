-- Staging model for orders

SELECT
    order_id,
    customer_id,
    order_date,
    status,
    updated_at,
    current_timestamp() as _loaded_at
FROM {{ source('silver', 'orders') }}
