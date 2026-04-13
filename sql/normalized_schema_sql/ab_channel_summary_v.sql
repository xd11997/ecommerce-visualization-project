-- ab_channel_summary_v source

CREATE VIEW ab_channel_summary_v AS
WITH expo AS (
  SELECT e.user_id, e.grp, u.channel
  FROM exposures e
  JOIN users u USING(user_id)
),
buyers AS (
  SELECT o.user_id, SUM(o.revenue) AS rev
  FROM orders o
  GROUP BY o.user_id
)
SELECT x.grp, x.channel,
       COUNT(*) AS exposed,
       SUM(CASE WHEN b.user_id IS NOT NULL THEN 1 ELSE 0 END) AS buyers,
       ROUND(1.0*SUM(CASE WHEN b.user_id IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),4) AS cr,
       ROUND(1.0*COALESCE(SUM(b.rev),0)/COUNT(*),2) AS arpu
FROM expo x LEFT JOIN buyers b ON b.user_id=x.user_id
GROUP BY x.grp, x.channel;
