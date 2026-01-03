-- Staging model for customers
-- Cleans and prepares customer data for dimensional modeling

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    address,
    updated_at,
    current_timestamp() as _loaded_at
FROM {{ source('silver', 'customers') }}
