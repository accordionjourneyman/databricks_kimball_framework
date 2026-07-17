-- SQL pattern only: framework does not yet schedule or validate this pattern.
-- Grain: one product per snapshot_date.
CREATE OR REPLACE TABLE demo_gold.fact_inventory_daily USING DELTA AS
SELECT
  snapshot_date,
  product_id,
  warehouse_id,
  SUM(on_hand_quantity) AS on_hand_quantity
FROM silver.inventory_movements
GROUP BY snapshot_date, product_id, warehouse_id;
