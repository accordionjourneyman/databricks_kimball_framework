-- SQL pattern only: framework does not yet manage milestone lifecycle updates.
-- Grain: one order.
MERGE INTO demo_gold.fact_order_lifecycle AS target
USING silver.order_events AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
  ordered_at = COALESCE(target.ordered_at, source.ordered_at),
  shipped_at = COALESCE(target.shipped_at, source.shipped_at),
  delivered_at = COALESCE(target.delivered_at, source.delivered_at)
WHEN NOT MATCHED THEN INSERT (order_id, ordered_at, shipped_at, delivered_at)
VALUES (source.order_id, source.ordered_at, source.shipped_at, source.delivered_at);
