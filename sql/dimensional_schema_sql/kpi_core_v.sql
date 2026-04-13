CREATE VIEW kpi_core_v AS
WITH p AS (
  SELECT date('2025-01-01') AS start_dt,
         date('2025-12-31') AS end_dt
),
events_in AS (
  SELECT e.user_key
  FROM fact_events e
  JOIN dim_date d
    ON d.date_key = e.date_key, p
  WHERE d.date >= p.start_dt
    AND d.date < date(p.end_dt, '+1 day')
),
active_users AS (
  SELECT COUNT(DISTINCT user_key) AS users
  FROM events_in
),
orders_in AS (
  SELECT order_key, user_key
  FROM fact_orders o
  JOIN dim_date d
    ON d.date_key = o.date_key, p
  WHERE d.date >= p.start_dt
    AND d.date < date(p.end_dt, '+1 day')
),
gmv AS (
  SELECT SUM(oi.line_amount) AS gmv
  FROM fact_order_items oi
  JOIN orders_in o
    ON o.order_key = oi.order_key
),
buyers AS (
  SELECT COUNT(DISTINCT user_key) AS buyer_cnt
  FROM orders_in
)
SELECT 'Users' AS metric, CAST(u.users AS REAL) AS value
FROM active_users u
UNION ALL
SELECT 'GMV' AS metric, COALESCE(g.gmv, 0.0) AS value
FROM gmv g
UNION ALL
SELECT 'Conversion' AS metric,
       CASE WHEN u.users = 0 THEN 0.0 ELSE 1.0 * b.buyer_cnt / u.users END AS value
FROM buyers b
CROSS JOIN active_users u
UNION ALL
SELECT 'ARPU' AS metric,
       CASE WHEN u.users = 0 THEN 0.0 ELSE COALESCE(g.gmv, 0.0) / u.users END AS value
FROM gmv g
CROSS JOIN active_users u;
