-- quarterly_category_share_v source

CREATE VIEW quarterly_category_share_v AS
WITH base AS (
  SELECT
    printf('%s-Q%d',
           strftime('%Y', o.order_dt),
           ((CAST(strftime('%m', o.order_dt) AS INT) - 1) / 3) + 1
    ) AS qtr,
    pp.category AS category,                 -- ✅ 使用products_pretty_v里的品类名
    SUM(oi.line_amount) AS gmv
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  JOIN products_pretty_v pp ON pp.sku_id = oi.sku_id
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