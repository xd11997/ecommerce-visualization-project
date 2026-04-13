-- monthly_gmv_stats_v source

CREATE VIEW monthly_gmv_stats_v AS
WITH p AS (
  SELECT date('2025-01-01') AS start_dt,
         date('2025-12-31') AS end_dt
),
orders_in AS (
  SELECT o.order_id, o.order_dt
  FROM orders o, p
  WHERE o.order_dt >= p.start_dt
    AND o.order_dt <  date(p.end_dt, '+1 day')
),
mth AS (  -- 每月 GMV
  SELECT
      date(strftime('%Y-%m-01', o.order_dt))               AS month_start,  -- 月首，用于时间轴
      CAST(strftime('%m', o.order_dt) AS INTEGER)          AS month_num,    -- 排序字段(1-12)
      strftime('%Y-%m', o.order_dt)                        AS month_label,  -- 显示标签
      SUM(oi.line_amount)                                  AS gmv
  FROM order_items oi
  JOIN orders_in o USING(order_id)
  GROUP BY month_start, month_num, month_label
),
s AS (  -- 年度统计（用于参照线）
  SELECT MAX(gmv) AS gmv_max,
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
    CASE WHEN m.gmv = s.gmv_max THEN 1 ELSE 0 END AS is_max_month  -- 可选：高亮最高月
FROM mth m CROSS JOIN s
ORDER BY m.month_start;
