-- kpi_core_v source

CREATE VIEW kpi_core_v AS
WITH p AS (
  SELECT date('2025-01-01') AS start_dt,
         date('2025-12-31') AS end_dt
),
events_in AS (
  SELECT e.user_id
  FROM events e, p
  WHERE e.ts >= p.start_dt AND e.ts < date(p.end_dt, '+1 day')
),
active_users AS (
  SELECT COUNT(DISTINCT user_id) AS users
  FROM events_in
),
orders_in AS (
  SELECT o.order_id, o.user_id, o.order_dt
  FROM orders o, p
  WHERE o.order_dt >= p.start_dt AND o.order_dt < date(p.end_dt, '+1 day')
),
gmv AS (
  SELECT SUM(oi.line_amount) AS gmv
  FROM order_items oi
  JOIN orders_in o USING(order_id)
),
buyers AS (
  SELECT COUNT(DISTINCT user_id) AS buyer_cnt
  FROM orders_in
)
SELECT 'Users'      AS metric, CAST(u.users AS REAL)                       AS value FROM active_users u
UNION ALL
SELECT 'GMV'        AS metric, COALESCE(g.gmv, 0.0)                        AS value FROM gmv g
UNION ALL
SELECT 'Conversion' AS metric, CASE WHEN u.users=0 THEN 0.0
                                   ELSE 1.0 * b.buyer_cnt / u.users END   AS value
FROM buyers b CROSS JOIN active_users u
UNION ALL
SELECT 'ARPU'       AS metric, CASE WHEN u.users=0 THEN 0.0
                                   ELSE COALESCE(g.gmv,0.0) / u.users END  AS value
FROM gmv g CROSS JOIN active_users u;
