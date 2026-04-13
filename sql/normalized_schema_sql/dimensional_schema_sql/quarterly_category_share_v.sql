CREATE VIEW quarterly_category_share_v AS
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
    printf('%d-Q%d', d.year, d.quarter) AS qtr,
    COALESCE(cl.category, p.category) AS category,
    SUM(oi.line_amount) AS gmv
  FROM fact_order_items oi
  JOIN dim_date d
    ON d.date_key = oi.date_key
  JOIN dim_products p
    ON p.sku_key = oi.sku_key
  LEFT JOIN category_labels cl
    ON cl.raw_category = p.category
  GROUP BY 1, 2
),
share_calc AS (
  SELECT
    qtr,
    category,
    gmv,
    gmv * 1.0 / SUM(gmv) OVER (PARTITION BY qtr) AS share
  FROM base
)
SELECT
  qtr,
  category,
  ROUND(gmv, 2) AS gmv,
  ROUND(share * 100, 2) AS share_pct
FROM share_calc
ORDER BY qtr, share_pct DESC;
