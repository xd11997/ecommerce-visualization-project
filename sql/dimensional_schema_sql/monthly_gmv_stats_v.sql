CREATE VIEW monthly_gmv_stats_v AS
WITH p AS (
  SELECT date('2025-01-01') AS start_dt,
         date('2025-12-31') AS end_dt
),
mth AS (
  SELECT
    date(strftime('%Y-%m-01', d.date)) AS month_start,
    d.month AS month_num,
    strftime('%Y-%m', d.date) AS month_label,
    SUM(oi.line_amount) AS gmv
  FROM fact_order_items oi
  JOIN dim_date d
    ON d.date_key = oi.date_key, p
  WHERE d.date >= p.start_dt
    AND d.date < date(p.end_dt, '+1 day')
  GROUP BY month_start, month_num, month_label
),
s AS (
  SELECT
    MAX(gmv) AS gmv_max,
    AVG(gmv) AS gmv_avg,
    MIN(gmv) AS gmv_min
  FROM mth
)
SELECT
  m.month_start,
  m.month_num,
  m.month_label,
  m.gmv,
  s.gmv_max,
  s.gmv_avg,
  s.gmv_min,
  CASE WHEN m.gmv = s.gmv_max THEN 1 ELSE 0 END AS is_max_month
FROM mth m
CROSS JOIN s
ORDER BY m.month_start;
