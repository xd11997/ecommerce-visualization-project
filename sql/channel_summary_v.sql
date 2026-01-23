-- channel_summary_v source

CREATE VIEW channel_summary_v AS
SELECT
    channel,
    SUM(gmv)                  AS gmv,
    SUM(orders)               AS order_count,
    SUM(users)                AS total_users,
    CASE WHEN SUM(users)=0 THEN 0.0
         ELSE 1.0 * SUM(orders) / SUM(users) END AS conversion_rate,
    CASE WHEN SUM(users)=0 THEN 0.0
         ELSE 1.0 * SUM(gmv) / SUM(users) END   AS arpu
FROM ab_channel_fact
GROUP BY channel
ORDER BY gmv DESC;