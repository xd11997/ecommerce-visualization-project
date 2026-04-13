-- channel_summary_v source

CREATE VIEW channel_summary_v AS
WITH user_base AS (
  SELECT
      u.channel,
      COUNT(*) AS users
  FROM users u
  GROUP BY u.channel
),
order_base AS (
  SELECT
      u.channel,
      COUNT(DISTINCT o.order_id) AS orders,
      COALESCE(SUM(o.revenue), 0.0) AS gmv
  FROM users u
  LEFT JOIN orders o ON o.user_id = u.user_id
  GROUP BY u.channel
)
SELECT
    ub.channel,
    ob.gmv AS gmv,
    ob.orders AS order_count,
    ub.users AS total_users,
    CASE WHEN ub.users=0 THEN 0.0
         ELSE 1.0 * ob.orders / ub.users END AS conversion_rate,
    CASE WHEN ub.users=0 THEN 0.0
         ELSE 1.0 * ob.gmv / ub.users END   AS arpu
FROM user_base ub
JOIN order_base ob USING(channel)
ORDER BY gmv DESC;
