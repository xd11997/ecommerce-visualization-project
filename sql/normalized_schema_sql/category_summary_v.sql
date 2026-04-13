-- category_summary_v source

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
      COALESCE(cl.category, raw.category) AS category,
      rev,
      ROUND(
        1.0 * SUM(rev) OVER (ORDER BY rev DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        / SUM(rev) OVER (),
        4
      ) AS cum_share
  FROM (
    SELECT
        p.category,
        SUM(oi.line_amount) AS rev
    FROM order_items oi
    JOIN products p USING(sku_id)
    GROUP BY p.category
  ) raw
  LEFT JOIN category_labels cl
    ON cl.raw_category = raw.category
),
total AS (
  SELECT SUM(rev) AS total_rev FROM base
),
ranked AS (
  SELECT
      b.category,
      b.rev,
      ROUND(100.0 * b.rev / t.total_rev, 2) AS pct_of_total,
      b.cum_share,
      ROW_NUMBER() OVER (ORDER BY b.rev DESC) AS rank
  FROM base b CROSS JOIN total t
)
SELECT
    category,
    rev,
    pct_of_total,
    cum_share,
    CASE WHEN rank <= 3 THEN 'Top 3'
         ELSE 'Other' END AS tier
FROM ranked
ORDER BY rev DESC;
