from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

from .common import PipelineConfig, ensure_dir, normalized_db_path, raw_dir, resolve_run_month


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE users (
  user_id     INTEGER PRIMARY KEY,
  signup_dt   DATE NOT NULL,
  channel     TEXT NOT NULL CHECK (channel IN ('ads','seo','social','referral','direct'))
);
CREATE TABLE products (
  sku_id      INTEGER PRIMARY KEY,
  sku_name    TEXT NOT NULL,
  category    TEXT NOT NULL,
  price       REAL NOT NULL
);
CREATE TABLE orders (
  order_id    INTEGER PRIMARY KEY,
  user_id     INTEGER NOT NULL REFERENCES users(user_id),
  order_dt    DATETIME NOT NULL,
  revenue     REAL NOT NULL
);
CREATE TABLE order_items (
  order_item_id INTEGER PRIMARY KEY,
  order_id    INTEGER NOT NULL REFERENCES orders(order_id),
  sku_id      INTEGER NOT NULL REFERENCES products(sku_id),
  qty         INTEGER NOT NULL,
  line_amount REAL NOT NULL
);
CREATE TABLE events (
  event_id    INTEGER PRIMARY KEY,
  user_id     INTEGER NOT NULL REFERENCES users(user_id),
  ts          DATETIME NOT NULL,
  event_type  TEXT NOT NULL,
  detail      TEXT
);
CREATE TABLE exposures (
  user_id   INTEGER NOT NULL REFERENCES users(user_id),
  grp       TEXT NOT NULL CHECK (grp IN ('A','B')),
  exp_ts    DATETIME NOT NULL
);

CREATE INDEX idx_orders_user_dt ON orders(user_id, order_dt);
CREATE INDEX idx_events_user_ts ON events(user_id, ts);
CREATE INDEX idx_items_order ON order_items(order_id);
CREATE INDEX idx_users_signup ON users(signup_dt);
CREATE INDEX idx_orders_dt ON orders(order_dt);
CREATE INDEX idx_events_type_ts ON events(event_type, ts);
CREATE INDEX idx_items_sku ON order_items(sku_id);
"""


TABLE_COLUMNS = {
    "users": ("user_id", "signup_dt", "channel"),
    "products": ("sku_id", "sku_name", "category", "price"),
    "orders": ("order_id", "user_id", "order_dt", "revenue"),
    "order_items": ("order_item_id", "order_id", "sku_id", "qty", "line_amount"),
    "events": ("event_id", "user_id", "ts", "event_type", "detail"),
    "exposures": ("user_id", "grp", "exp_ts"),
}


def ingest_csv_to_sqlite(run_month: str | None = None) -> Path:
    month_tag = resolve_run_month(run_month)
    input_dir = raw_dir(month_tag)
    output_db = normalized_db_path(month_tag)
    ensure_dir(output_db.parent)
    if output_db.exists():
        output_db.unlink()

    conn = sqlite3.connect(output_db)
    try:
        conn.executescript(SCHEMA_SQL)
        for table_name, columns in TABLE_COLUMNS.items():
            csv_path = input_dir / f"{table_name}.csv"
            load_table(conn, table_name, columns, csv_path)
        conn.commit()
    finally:
        conn.close()
    return output_db


def load_table(conn: sqlite3.Connection, table_name: str, columns: tuple[str, ...], csv_path: Path) -> None:
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        batch: list[tuple[str, ...]] = []
        for row in reader:
            batch.append(tuple(row[column] for column in columns))
            if len(batch) >= 10_000:
                conn.executemany(sql, batch)
                batch.clear()
        if batch:
            conn.executemany(sql, batch)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw CSV files into a normalized SQLite database.")
    parser.add_argument("--run-month", help="Input/output folder month in YYYY-MM format.")
    args = parser.parse_args()
    db_path = ingest_csv_to_sqlite(args.run_month)
    print(f"Normalized database created at {db_path}")


if __name__ == "__main__":
    main()
