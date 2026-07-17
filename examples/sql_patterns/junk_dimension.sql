-- Reference SQL equivalent of the framework-managed junk-dimension lifecycle.
-- Grain: one distinct combination of the declared low-cardinality flags.
CREATE OR REPLACE TABLE demo_gold.dim_order_flags USING DELTA AS
SELECT
  xxhash64(is_gift, is_priority, is_first_order) AS order_flags_sk,
  is_gift,
  is_priority,
  is_first_order
FROM silver.orders
GROUP BY is_gift, is_priority, is_first_order;
