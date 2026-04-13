-- sku_by_category_v source

CREATE VIEW sku_by_category_v AS
WITH category_labels AS (
  SELECT 'cat_1' AS raw_category, 'Apparel' AS category
  UNION ALL SELECT 'cat_2', 'Beauty'
  UNION ALL SELECT 'cat_3', 'Home'
  UNION ALL SELECT 'cat_4', 'Electronics'
  UNION ALL SELECT 'cat_5', 'Sports'
  UNION ALL SELECT 'cat_6', 'Other'
),
items AS (
  SELECT oi.sku_id, oi.line_amount
  FROM order_items oi
  JOIN orders o ON o.order_id = oi.order_id
),
sku_rev AS (
  SELECT
      pr.sku_id,
      pr.sku_name,
      COALESCE(cl.category, pr.category) AS category,
      SUM(i.line_amount) AS rev
  FROM items i JOIN products pr USING(sku_id)
  LEFT JOIN category_labels cl ON cl.raw_category = pr.category
  GROUP BY 1,2,3
),
acc AS (
  SELECT category, sku_name, rev,
         SUM(rev) OVER (PARTITION BY category ORDER BY rev DESC) AS cum_rev_cat,
         SUM(rev) OVER (PARTITION BY category)                 AS tot_rev_cat
  FROM sku_rev
)
SELECT category, sku_name, rev,
       ROUND(1.0*cum_rev_cat/tot_rev_cat,4) AS cum_share_in_cat
FROM acc
ORDER BY category, rev DESC;
