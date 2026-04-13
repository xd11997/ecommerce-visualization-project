from __future__ import annotations

import argparse

from .common import PipelineConfig, RAW_TABLES, dump_manifest, ensure_dir, raw_dir, resolve_run_month, write_csv
from .raw_data_generator import (
    build_dataset,
    event_rows,
    exposure_rows,
    order_item_rows,
    order_rows,
    product_rows,
    user_rows,
)


def export_raw_csv(config: PipelineConfig) -> dict[str, int]:
    run_month = resolve_run_month(config.run_month)
    output_dir = ensure_dir(raw_dir(run_month))
    dataset = build_dataset(config)

    counts = {
        "users": write_csv(output_dir / "users.csv", ["user_id", "signup_dt", "channel"], user_rows(dataset)),
        "products": write_csv(output_dir / "products.csv", ["sku_id", "sku_name", "category", "price"], product_rows(dataset)),
        "orders": write_csv(output_dir / "orders.csv", ["order_id", "user_id", "order_dt", "revenue"], order_rows(dataset)),
        "order_items": write_csv(
            output_dir / "order_items.csv",
            ["order_item_id", "order_id", "sku_id", "qty", "line_amount"],
            order_item_rows(dataset),
        ),
        "events": write_csv(output_dir / "events.csv", ["event_id", "user_id", "ts", "event_type", "detail"], event_rows(dataset)),
        "exposures": write_csv(output_dir / "exposures.csv", ["user_id", "grp", "exp_ts"], exposure_rows(dataset)),
    }
    dump_manifest(run_month, config, counts)
    return counts


def parse_args() -> PipelineConfig:
    parser = argparse.ArgumentParser(description="Generate synthetic raw ecommerce CSVs.")
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
    config = parse_args()
    counts = export_raw_csv(config)
    print(f"Raw CSV export complete for {resolve_run_month(config.run_month)}: {counts}")


if __name__ == "__main__":
    main()
