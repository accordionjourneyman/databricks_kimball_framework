-- Staging model for order items

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    sales_amount,
    current_timestamp() as _loaded_at
FROM {{ source('silver', 'order_items') }}
