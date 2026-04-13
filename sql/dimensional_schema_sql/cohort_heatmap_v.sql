CREATE VIEW cohort_heatmap_v AS
WITH base AS (
  SELECT
    user_key,
    strftime('%Y-%m', signup_dt) AS cohort_month,
    CAST(strftime('%Y', signup_dt) AS INTEGER) * 12 + CAST(strftime('%m', signup_dt) AS INTEGER) AS cohort_ym
  FROM dim_users
)
SELECT
  b.cohort_month,
  (d.year * 12 + d.month - b.cohort_ym) AS month_age,
  COUNT(DISTINCT o.user_key) AS active_users
FROM fact_orders o
JOIN dim_date d
  ON d.date_key = o.date_key
JOIN base b
  ON b.user_key = o.user_key
WHERE (d.year * 12 + d.month - b.cohort_ym) >= 0
GROUP BY b.cohort_month, month_age;
