-- category_summary_v source

CREATE VIEW category_summary_v AS
WITH base AS (
  SELECT category, rev, cum_share
  FROM category_pareto_v
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