CREATE VIEW sku_by_category_v AS
WITH category_labels AS (
  SELECT 'cat_1' AS raw_category, 'Apparel' AS category
  UNION ALL SELECT 'cat_2', 'Beauty'
  UNION ALL SELECT 'cat_3', 'Home'
  UNION ALL SELECT 'cat_4', 'Electronics'
  UNION ALL SELECT 'cat_5', 'Sports'
  UNION ALL SELECT 'cat_6', 'Other'
),
sku_rev AS (
  SELECT
    COALESCE(cl.category, p.category) AS category,
    p.sku_name,
    SUM(oi.line_amount) AS rev
  FROM fact_order_items oi
  JOIN dim_products p
    ON p.sku_key = oi.sku_key
  LEFT JOIN category_labels cl
    ON cl.raw_category = p.category
  GROUP BY COALESCE(cl.category, p.category), p.sku_name
),
acc AS (
  SELECT
    category,
    sku_name,
    rev,
    SUM(rev) OVER (PARTITION BY category ORDER BY rev DESC) AS cum_rev_cat,
    SUM(rev) OVER (PARTITION BY category) AS tot_rev_cat
  FROM sku_rev
)
SELECT
  category,
  sku_name,
  rev,
  ROUND(1.0 * cum_rev_cat / tot_rev_cat, 4) AS cum_share_in_cat
FROM acc
ORDER BY category, rev DESC;
