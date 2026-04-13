CREATE VIEW ab_channel_summary_v AS
WITH expo AS (
  SELECT
    e.user_key,
    e.grp,
    u.channel
  FROM fact_exposures e
  JOIN dim_users u
    ON u.user_key = e.user_key
),
buyers AS (
  SELECT
    user_key,
    SUM(revenue) AS rev
  FROM fact_orders
  GROUP BY user_key
)
SELECT
  x.grp,
  x.channel,
  COUNT(*) AS exposed,
  SUM(CASE WHEN b.user_key IS NOT NULL THEN 1 ELSE 0 END) AS buyers,
  ROUND(1.0 * SUM(CASE WHEN b.user_key IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS cr,
  ROUND(1.0 * COALESCE(SUM(b.rev), 0) / COUNT(*), 2) AS arpu
FROM expo x
LEFT JOIN buyers b
  ON b.user_key = x.user_key
GROUP BY x.grp, x.channel;
