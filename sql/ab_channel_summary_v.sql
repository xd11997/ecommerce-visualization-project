-- ab_channel_summary_v source

CREATE VIEW ab_channel_summary_v AS
WITH p AS (SELECT * FROM params),
expo AS (SELECT e.user_id, e.grp, u.channel FROM exposures e JOIN users u USING(user_id)),
buyers AS (
  SELECT o.user_id, SUM(o.revenue) AS rev
  FROM orders o, p
  WHERE date(o.order_dt) BETWEEN p.start_date AND p.end_date
  GROUP BY o.user_id
)
SELECT x.grp, x.channel,
       COUNT(DISTINCT x.user_id) AS exposed,
       COUNT(DISTINCT b.user_id) AS buyers,
       ROUND(1.0*COUNT(DISTINCT b.user_id)/COUNT(DISTINCT x.user_id),4) AS cr,
       ROUND(1.0*COALESCE(SUM(b.rev),0)/COUNT(DISTINCT x.user_id),2) AS arpu
FROM expo x LEFT JOIN buyers b ON b.user_id=x.user_id
GROUP BY x.grp, x.channel;