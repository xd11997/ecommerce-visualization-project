from __future__ import annotations

import random
from bisect import bisect
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from itertools import accumulate
from typing import Iterator

from .common import PipelineConfig


CHANNELS = ["ads", "seo", "social", "referral", "direct"]
CATEGORIES = ["cat_1", "cat_2", "cat_3", "cat_4", "cat_5", "cat_6"]
EVENT_TYPES = ["session_start", "page_view", "add_to_cart", "checkout_start", "purchase", "email_click"]


@dataclass(slots=True)
class GeneratedDataset:
    rng: random.Random
    config: PipelineConfig
    signup_dates: list[date]
    channels: list[str]
    product_categories: list[str]
    product_prices: list[float]
    user_cum_weights: list[float]


def build_dataset(config: PipelineConfig) -> GeneratedDataset:
    seed = config.seed if config.seed is not None else random.SystemRandom().randrange(1, 10**9)
    rng = random.Random(seed)
    config.seed = seed

    signup_dates = [random_signup_date(rng) for _ in range(config.users)]
    channels = [weighted_choice(rng, CHANNELS, [0.24, 0.18, 0.2, 0.14, 0.24]) for _ in range(config.users)]
    product_categories = [rng.choice(CATEGORIES) for _ in range(config.products)]
    product_prices = [round(rng.uniform(8, 280), 2) for _ in range(config.products)]
    user_cum_weights = list(accumulate(user_order_weights(channels)))

    return GeneratedDataset(
        rng=rng,
        config=config,
        signup_dates=signup_dates,
        channels=channels,
        product_categories=product_categories,
        product_prices=product_prices,
        user_cum_weights=user_cum_weights,
    )


def random_signup_date(rng: random.Random) -> date:
    start = date(2025, 1, 1)
    return start + timedelta(days=rng.randint(0, 364))


def weighted_choice(rng: random.Random, values: list[str], weights: list[float]) -> str:
    return rng.choices(values, weights=weights, k=1)[0]


def random_timestamp(rng: random.Random, day: date) -> str:
    dt_value = datetime.combine(day, time.min) + timedelta(
        hours=rng.randint(0, 23),
        minutes=rng.randint(0, 59),
        seconds=rng.randint(0, 59),
    )
    return dt_value.strftime("%Y-%m-%d %H:%M:%S")


def order_timestamp(rng: random.Random, signup_day: date) -> str:
    latest = date(2025, 12, 31)
    if signup_day >= latest:
        return random_timestamp(rng, latest)
    day = signup_day + timedelta(days=rng.randint(0, (latest - signup_day).days))
    return random_timestamp(rng, day)


def user_rows(dataset: GeneratedDataset) -> Iterator[tuple[int, str, str]]:
    for idx, signup_dt in enumerate(dataset.signup_dates, start=1):
        yield idx, signup_dt.isoformat(), dataset.channels[idx - 1]


def product_rows(dataset: GeneratedDataset) -> Iterator[tuple[int, str, str, float]]:
    for sku_id, price in enumerate(dataset.product_prices, start=1):
        category = dataset.product_categories[sku_id - 1]
        yield sku_id, f"sku_{category}_{sku_id:04d}", category, price


def exposure_rows(dataset: GeneratedDataset) -> Iterator[tuple[int, str, str]]:
    for user_id, signup_dt in enumerate(dataset.signup_dates, start=1):
        exposure_day = signup_dt + timedelta(days=dataset.rng.randint(0, min(7, 365 - signup_dt.timetuple().tm_yday)))
        if exposure_day.year != 2025:
            exposure_day = date(2025, 12, 31)
        grp = dataset.rng.choice(["A", "B"])
        yield user_id, grp, random_timestamp(dataset.rng, exposure_day)


def order_rows(dataset: GeneratedDataset) -> Iterator[tuple[int, int, str, float]]:
    for order_id in range(1, dataset.config.orders + 1):
        user_id = pick_weighted_user(dataset)
        signup_dt = dataset.signup_dates[user_id - 1]
        revenue = round(dataset.rng.uniform(15, 420), 2)
        yield order_id, user_id, order_timestamp(dataset.rng, signup_dt), revenue


def order_item_rows(dataset: GeneratedDataset) -> Iterator[tuple[int, int, int, int, float]]:
    order_item_id = 1
    for order_id in range(1, dataset.config.orders + 1):
        item_count = dataset.rng.randint(dataset.config.order_items_min, dataset.config.order_items_max)
        for _ in range(item_count):
            sku_id = dataset.rng.randint(1, dataset.config.products)
            qty = dataset.rng.randint(1, 3)
            price = dataset.product_prices[sku_id - 1]
            multiplier = 1 + dataset.rng.uniform(-0.08, 0.12)
            line_amount = round(price * qty * multiplier, 2)
            yield order_item_id, order_id, sku_id, qty, line_amount
            order_item_id += 1


def event_rows(dataset: GeneratedDataset) -> Iterator[tuple[int, int, str, str, str]]:
    for event_id in range(1, dataset.config.events + 1):
        user_id = dataset.rng.randint(1, dataset.config.users)
        signup_dt = dataset.signup_dates[user_id - 1]
        event_type = weighted_choice(
            dataset.rng,
            EVENT_TYPES,
            [0.14, 0.42, 0.16, 0.1, 0.05, 0.13],
        )
        ts = order_timestamp(dataset.rng, signup_dt)
        detail = build_event_detail(event_type, dataset.rng)
        yield event_id, user_id, ts, event_type, detail


def build_event_detail(event_type: str, rng: random.Random) -> str:
    if event_type == "page_view":
        return rng.choice(["home", "category", "product", "promo"])
    if event_type == "add_to_cart":
        return f"sku:{rng.randint(1, 9999)}"
    if event_type == "checkout_start":
        return "checkout"
    if event_type == "purchase":
        return f"order_complete:{rng.randint(1, 999999)}"
    if event_type == "email_click":
        return rng.choice(["promo_a", "promo_b", "winback", "newsletter"])
    return "session"


def user_order_weights(channels: list[str]) -> list[float]:
    channel_weights = {
        "ads": 1.1,
        "seo": 0.95,
        "social": 0.9,
        "referral": 1.2,
        "direct": 1.05,
    }
    return [channel_weights[channel] for channel in channels]


def pick_weighted_user(dataset: GeneratedDataset) -> int:
    threshold = dataset.rng.uniform(0, dataset.user_cum_weights[-1])
    return bisect(dataset.user_cum_weights, threshold) + 1
