-- sku_by_category_v source

CREATE VIEW sku_by_category_v AS
WITH p AS (SELECT * FROM params),
items AS (
  SELECT oi.sku_id, oi.line_amount
  FROM order_items oi
  JOIN orders o ON o.order_id = oi.order_id, p
  WHERE date(o.order_dt) BETWEEN p.start_date AND p.end_date
),
sku_rev AS (
  SELECT pr.sku_id, pr.sku_name, pr.category, SUM(i.line_amount) AS rev
  FROM items i JOIN products_pretty_v pr USING(sku_id)
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