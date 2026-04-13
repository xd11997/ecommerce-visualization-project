from __future__ import annotations

import argparse
from dataclasses import replace

from .common import PipelineConfig, resolve_run_month
from .export_csv import export_raw_csv
from .ingest import ingest_csv_to_sqlite
from .transform import transform_to_star


def run_pipeline(config: PipelineConfig) -> None:
    run_month = resolve_run_month(config.run_month)
    stage_config = replace(config, run_month=run_month)

    print(f"[1/3] Generating raw CSV data for {run_month}...")
    raw_counts = export_raw_csv(stage_config)
    print(f"Raw files ready: {raw_counts}")

    print(f"[2/3] Building normalized SQLite storage for {run_month}...")
    normalized_db = ingest_csv_to_sqlite(run_month)
    print(f"Normalized database ready: {normalized_db}")

    print(f"[3/3] Transforming to star schema and exporting view CSVs for {run_month}...")
    star_db = transform_to_star(run_month)
    print(f"Star database and views ready: {star_db}")


def parse_args() -> PipelineConfig:
    parser = argparse.ArgumentParser(description="Run the full synthetic ecommerce pipeline.")
    parser.add_argument("--run-month", help="Output folder month in YYYY-MM format.")
    parser.add_argument("--users", type=int, default=50_000)
    parser.add_argument("--products", type=int, default=250)
    parser.add_argument("--orders", type=int, default=150_000)
    parser.add_argument("--order-items-min", type=int, default=1)
    parser.add_argument("--order-items-max", type=int, default=4)
    parser.add_argument("--events", type=int, default=300_000)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    return PipelineConfig(
        users=args.users,
        products=args.products,
        orders=args.orders,
        order_items_min=args.order_items_min,
        order_items_max=args.order_items_max,
        events=args.events,
        run_month=args.run_month,
        seed=args.seed,
    )


def main() -> None:
    run_pipeline(parse_args())


if __name__ == "__main__":
    main()
