CREATE VIEW category_summary_v AS
WITH category_labels AS (
  SELECT 'cat_1' AS raw_category, 'Apparel' AS category
  UNION ALL SELECT 'cat_2', 'Beauty'
  UNION ALL SELECT 'cat_3', 'Home'
  UNION ALL SELECT 'cat_4', 'Electronics'
  UNION ALL SELECT 'cat_5', 'Sports'
  UNION ALL SELECT 'cat_6', 'Other'
),
base AS (
  SELECT
    COALESCE(cl.category, p.category) AS category,
    SUM(f.line_amount) AS rev
  FROM fact_order_items f
  JOIN dim_products p
    ON p.sku_key = f.sku_key
  LEFT JOIN category_labels cl
    ON cl.raw_category = p.category
  GROUP BY COALESCE(cl.category, p.category)
),
ranked AS (
  SELECT
    category,
    rev,
    ROUND(100.0 * rev / SUM(rev) OVER (), 2) AS pct_of_total,
    ROUND(
      1.0 * SUM(rev) OVER (ORDER BY rev DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      / SUM(rev) OVER (),
      4
    ) AS cum_share,
    ROW_NUMBER() OVER (ORDER BY rev DESC) AS rank
  FROM base
)
SELECT
  category,
  rev,
  pct_of_total,
  cum_share,
  CASE WHEN rank <= 3 THEN 'Top 3' ELSE 'Other' END AS tier
FROM ranked
ORDER BY rev DESC;
