-- basket_pairs_top20_v source

CREATE VIEW basket_pairs_top20_v AS
WITH items AS (
  SELECT oi.order_id, oi.sku_id
  FROM order_items oi
  JOIN orders o ON o.order_id=oi.order_id
),
pairs AS (
  SELECT i1.sku_id AS sku_a, i2.sku_id AS sku_b, COUNT(*) AS cnt
  FROM items i1 JOIN items i2 ON i1.order_id=i2.order_id AND i1.sku_id<i2.sku_id
  GROUP BY 1,2
)
SELECT p.sku_name AS a_name, q.sku_name AS b_name, cnt
FROM pairs
JOIN products p ON p.sku_id=sku_a
JOIN products q ON q.sku_id=sku_b
ORDER BY cnt DESC
LIMIT 20;
