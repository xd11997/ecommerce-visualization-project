-- quarterly_category_share_v source

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
    printf('%s-Q%d',
           strftime('%Y', o.order_dt),
           ((CAST(strftime('%m', o.order_dt) AS INT) - 1) / 3) + 1
    ) AS qtr,
    COALESCE(cl.category, p.category) AS category,
    SUM(oi.line_amount) AS gmv
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  JOIN products p ON p.sku_id = oi.sku_id
  LEFT JOIN category_labels cl ON cl.raw_category = p.category
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
