from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

from .common import STAR_VIEW_FILES, ensure_dir, normalized_db_path, read_sql, resolve_run_month, star_db_path, views_dir


STAR_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE dim_users (
  user_key   INTEGER PRIMARY KEY,
  signup_dt  DATE NOT NULL,
  channel    TEXT NOT NULL CHECK (channel IN ('ads','seo','social','referral','direct'))
);

CREATE TABLE dim_date (
  date_key  INTEGER PRIMARY KEY,
  date      DATE NOT NULL UNIQUE,
  year      INTEGER NOT NULL,
  quarter   INTEGER NOT NULL,
  month     INTEGER NOT NULL,
  weekday   TEXT NOT NULL
);

CREATE TABLE dim_products (
  sku_key    INTEGER PRIMARY KEY,
  sku_name   TEXT NOT NULL,
  category   TEXT NOT NULL,
  price      REAL NOT NULL
);

CREATE TABLE fact_orders (
  order_key  INTEGER PRIMARY KEY,
  user_key   INTEGER NOT NULL REFERENCES dim_users(user_key),
  date_key   INTEGER NOT NULL REFERENCES dim_date(date_key),
  revenue    REAL NOT NULL
);

CREATE TABLE fact_order_items (
  order_item_key  INTEGER PRIMARY KEY,
  order_key       INTEGER NOT NULL REFERENCES fact_orders(order_key),
  sku_key         INTEGER NOT NULL REFERENCES dim_products(sku_key),
  date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
  user_key        INTEGER NOT NULL REFERENCES dim_users(user_key),
  qty             INTEGER NOT NULL,
  line_amount     REAL NOT NULL
);

CREATE TABLE fact_exposures (
  exposure_key  INTEGER PRIMARY KEY,
  user_key      INTEGER NOT NULL REFERENCES dim_users(user_key),
  date_key      INTEGER NOT NULL REFERENCES dim_date(date_key),
  grp           TEXT NOT NULL CHECK (grp IN ('A','B')),
  exp_ts        DATETIME NOT NULL
);

CREATE TABLE fact_events (
  event_key    INTEGER PRIMARY KEY,
  user_key     INTEGER NOT NULL REFERENCES dim_users(user_key),
  date_key     INTEGER NOT NULL REFERENCES dim_date(date_key),
  event_ts     DATETIME NOT NULL,
  event_type   TEXT NOT NULL,
  detail       TEXT
);

CREATE INDEX idx_dim_users_signup ON dim_users(signup_dt);
CREATE INDEX idx_dim_date_date ON dim_date(date);
CREATE INDEX idx_fact_orders_user_date ON fact_orders(user_key, date_key);
CREATE INDEX idx_fact_orders_date ON fact_orders(date_key);
CREATE INDEX idx_fact_items_order ON fact_order_items(order_key);
CREATE INDEX idx_fact_items_sku ON fact_order_items(sku_key);
CREATE INDEX idx_fact_items_user_date ON fact_order_items(user_key, date_key);
CREATE INDEX idx_fact_exposures_user_date ON fact_exposures(user_key, date_key);
CREATE INDEX idx_fact_events_user_date ON fact_events(user_key, date_key);
CREATE INDEX idx_fact_events_type_ts ON fact_events(event_type, event_ts);
"""


LOAD_STAR_SQL = """
INSERT INTO dim_users (user_key, signup_dt, channel)
SELECT user_id, signup_dt, channel
FROM src.users;

WITH RECURSIVE date_span(dt) AS (
  SELECT (
    SELECT MIN(d)
    FROM (
      SELECT MIN(date(signup_dt)) AS d FROM src.users
      UNION ALL
      SELECT MIN(date(order_dt)) FROM src.orders
      UNION ALL
      SELECT MIN(date(exp_ts)) FROM src.exposures
      UNION ALL
      SELECT MIN(date(ts)) FROM src.events
    )
  )
  UNION ALL
  SELECT date(dt, '+1 day')
  FROM date_span
  WHERE dt < (
    SELECT MAX(d)
    FROM (
      SELECT MAX(date(signup_dt)) AS d FROM src.users
      UNION ALL
      SELECT MAX(date(order_dt)) FROM src.orders
      UNION ALL
      SELECT MAX(date(exp_ts)) FROM src.exposures
      UNION ALL
      SELECT MAX(date(ts)) FROM src.events
    )
  )
)
INSERT INTO dim_date (date_key, date, year, quarter, month, weekday)
SELECT
  CAST(strftime('%Y%m%d', dt) AS INTEGER) AS date_key,
  dt AS date,
  CAST(strftime('%Y', dt) AS INTEGER) AS year,
  ((CAST(strftime('%m', dt) AS INTEGER) - 1) / 3) + 1 AS quarter,
  CAST(strftime('%m', dt) AS INTEGER) AS month,
  CASE strftime('%w', dt)
    WHEN '0' THEN 'Sunday'
    WHEN '1' THEN 'Monday'
    WHEN '2' THEN 'Tuesday'
    WHEN '3' THEN 'Wednesday'
    WHEN '4' THEN 'Thursday'
    WHEN '5' THEN 'Friday'
    ELSE 'Saturday'
  END AS weekday
FROM date_span;

INSERT INTO dim_products (sku_key, sku_name, category, price)
SELECT sku_id, sku_name, category, price
FROM src.products;

INSERT INTO fact_orders (order_key, user_key, date_key, revenue)
SELECT
  order_id,
  user_id,
  CAST(strftime('%Y%m%d', date(order_dt)) AS INTEGER) AS date_key,
  revenue
FROM src.orders;

INSERT INTO fact_order_items (order_item_key, order_key, sku_key, date_key, user_key, qty, line_amount)
SELECT
  oi.order_item_id,
  oi.order_id,
  oi.sku_id,
  CAST(strftime('%Y%m%d', date(o.order_dt)) AS INTEGER) AS date_key,
  o.user_id,
  oi.qty,
  oi.line_amount
FROM src.order_items oi
JOIN src.orders o
  ON o.order_id = oi.order_id;

INSERT INTO fact_exposures (exposure_key, user_key, date_key, grp, exp_ts)
SELECT
  ROW_NUMBER() OVER (ORDER BY e.user_id, e.exp_ts) AS exposure_key,
  e.user_id,
  CAST(strftime('%Y%m%d', date(e.exp_ts)) AS INTEGER) AS date_key,
  e.grp,
  e.exp_ts
FROM src.exposures e;

INSERT INTO fact_events (event_key, user_key, date_key, event_ts, event_type, detail)
SELECT
  event_id,
  user_id,
  CAST(strftime('%Y%m%d', date(ts)) AS INTEGER) AS date_key,
  ts,
  event_type,
  detail
FROM src.events;
"""


def transform_to_star(run_month: str | None = None) -> Path:
    month_tag = resolve_run_month(run_month)
    source_db = normalized_db_path(month_tag)
    target_db = star_db_path(month_tag)
    ensure_dir(target_db.parent)
    if target_db.exists():
        target_db.unlink()

    conn = sqlite3.connect(target_db)
    try:
        conn.executescript(STAR_SCHEMA_SQL)
        attach_path = str(source_db).replace("'", "''")
        conn.execute(f"ATTACH DATABASE '{attach_path}' AS src")
        conn.executescript(LOAD_STAR_SQL)
        conn.execute("DETACH DATABASE src")
        for view_file in STAR_VIEW_FILES:
            conn.executescript(read_sql(view_file))
        conn.commit()
    finally:
        conn.close()

    export_star_views(month_tag)
    return target_db


def export_star_views(run_month: str) -> None:
    output_dir = ensure_dir(views_dir(run_month))
    conn = sqlite3.connect(star_db_path(run_month))
    conn.row_factory = sqlite3.Row
    try:
        for view_file in STAR_VIEW_FILES:
            view_name = view_file.stem
            rows = conn.execute(f"SELECT * FROM {view_name}").fetchall()
            csv_path = output_dir / f"{view_name}.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                if rows:
                    writer.writerow(rows[0].keys())
                    for row in rows:
                        writer.writerow(tuple(row))
                else:
                    columns = [info[1] for info in conn.execute(f"PRAGMA table_info({view_name})").fetchall()]
                    writer.writerow(columns)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform normalized SQLite tables into star schema and exported views.")
    parser.add_argument("--run-month", help="Input/output folder month in YYYY-MM format.")
    args = parser.parse_args()
    star_path = transform_to_star(args.run_month)
    print(f"Star schema database created at {star_path}")


if __name__ == "__main__":
    main()
