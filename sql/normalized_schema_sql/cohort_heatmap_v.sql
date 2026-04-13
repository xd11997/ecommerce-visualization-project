-- cohort_heatmap_v source

CREATE VIEW cohort_heatmap_v AS
WITH o AS (
  SELECT user_id, date(order_dt) AS od
  FROM orders o
),
base AS (
  SELECT user_id,
         strftime('%Y-%m', signup_dt) AS cohort_month,
         CAST(strftime('%Y', signup_dt) AS INT)*12 + CAST(strftime('%m', signup_dt) AS INT) AS cohort_ym
  FROM users
)
SELECT b.cohort_month,
       (CAST(strftime('%Y', o.od) AS INT)*12 + CAST(strftime('%m', o.od) AS INT) - b.cohort_ym) AS month_age,
       COUNT(DISTINCT o.user_id) AS active_users
FROM o JOIN base b USING(user_id)
WHERE (CAST(strftime('%Y', o.od) AS INT)*12 + CAST(strftime('%m', o.od) AS INT) - b.cohort_ym) >= 0
GROUP BY b.cohort_month, month_age;
