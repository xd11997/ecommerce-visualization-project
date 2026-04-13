CREATE VIEW basket_pairs_top20_v AS
WITH pairs AS (
  SELECT
    i1.sku_key AS sku_a,
    i2.sku_key AS sku_b,
    COUNT(*) AS cnt
  FROM fact_order_items i1
  JOIN fact_order_items i2
    ON i1.order_key = i2.order_key
   AND i1.sku_key < i2.sku_key
  GROUP BY 1, 2
)
SELECT
  p.sku_name AS a_name,
  q.sku_name AS b_name,
  cnt
FROM pairs
JOIN dim_products p
  ON p.sku_key = sku_a
JOIN dim_products q
  ON q.sku_key = sku_b
ORDER BY cnt DESC
LIMIT 20;
