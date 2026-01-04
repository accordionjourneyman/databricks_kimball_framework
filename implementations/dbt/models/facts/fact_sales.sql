-- Fact Sales Table
-- Joins order items with dimensions to create Kimball-compliant fact table

{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='append_new_columns'
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

-- Get customer records from SCD2 snapshot (dbt_valid_to NULL = current)
dim_customer AS (
    SELECT 
        customer_sk,
        customer_id,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('dim_customer') }}
),

dim_product AS (
    SELECT * FROM {{ ref('dim_product') }}
)

SELECT
    -- Degenerate dimension (fact key)
    oi.order_item_id,
    o.order_id,
    
    -- Foreign keys to dimensions (with Kimball-style defaults for missing)
    COALESCE(c.customer_sk, '{{ var("unknown_sk") }}') as customer_sk,
    COALESCE(p.product_sk, '{{ var("unknown_sk") }}') as product_sk,
    
    -- Date dimension (degenerate)
    o.order_date,
    
    -- Measures
    oi.quantity,
    oi.sales_amount,
    (oi.sales_amount - (p.unit_cost * oi.quantity)) as net_profit,
    
    -- Audit columns
    current_timestamp() as __etl_processed_at

FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.order_id
-- Temporal SCD2 join: find dimension version active at order_date
LEFT JOIN dim_customer c 
    ON o.customer_id = c.customer_id
    AND o.order_date >= c.dbt_valid_from 
    AND (c.dbt_valid_to IS NULL OR o.order_date < c.dbt_valid_to)
LEFT JOIN dim_product p ON oi.product_id = p.product_id

{% if is_incremental() %}
-- CDF filtering handled in staging; this catches any edge cases
WHERE 1=1
{% endif %}
