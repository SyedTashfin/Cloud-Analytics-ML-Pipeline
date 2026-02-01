from __future__ import annotations

import argparse
import calendar
import csv
import logging
import random
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Sequence

from clickstream.config import ensure_dirs, load_config, resolve_paths

LOGGER = logging.getLogger(__name__)

COLUMNS = [
    "event_time",
    "user_id",
    "session_id",
    "event_type",
    "product_id",
    "price",
    "currency",
    "device_type",
    "browser",
    "referrer",
    "country",
    "session_duration",
    "pages_viewed",
    "month",
]


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Download or synthesize raw clickstream data."
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    return parser.parse_args()


def download_file(url: str, dest: Path) -> None:
    """Download a file to the target destination."""
    LOGGER.info("Downloading %s -> %s", url, dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, dest)


def choose_event_type(
    rng: random.Random, event_types: Sequence[str], weights: Sequence[float]
) -> str:
    """Sample an event type with optional weights."""
    if weights and len(weights) == len(event_types):
        return rng.choices(event_types, weights=weights, k=1)[0]
    return rng.choice(event_types)


def generate_synthetic_csv(
    dest: Path, month: str, dataset_cfg: Dict[str, Any], seed_offset: int
) -> None:
    """Generate a synthetic clickstream CSV file for a given month."""
    rows = int(dataset_cfg.get("rows_per_month", 10000))
    seed = int(dataset_cfg.get("seed", 0)) + seed_offset
    rng = random.Random(seed)

    event_types = dataset_cfg.get("event_types", ["view", "cart", "purchase"])
    event_weights = dataset_cfg.get("event_type_weights", [])
    currency = dataset_cfg.get("currency", "USD")

    device_types = ["desktop", "mobile", "tablet"]
    browsers = ["chrome", "safari", "firefox", "edge"]
    referrers = ["direct", "search", "social", "email", "ads"]
    countries = ["US", "CA", "GB", "DE", "IN"]

    month_start = datetime.strptime(f"{month}-01", "%Y-%m-%d")
    days_in_month = calendar.monthrange(month_start.year, month_start.month)[1]
    seconds_in_month = days_in_month * 24 * 60 * 60 - 1

    ensure_dirs(dest.parent)
    with dest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)

        for _ in range(rows):
            event_time = month_start + timedelta(
                seconds=rng.randint(0, seconds_in_month)
            )
            user_id = rng.randint(1, 50000)
            session_id = f"{user_id}-{rng.randint(100000, 999999)}"
            event_type = choose_event_type(rng, event_types, event_weights)
            product_id = rng.randint(10000, 11000)
            base_price = max(0.0, rng.gauss(60, 25))
            if event_type == "view":
                price = round(base_price * rng.random(), 2)
            else:
                price = round(base_price, 2)
            device_type = rng.choice(device_types)
            browser = rng.choice(browsers)
            referrer = rng.choice(referrers)
            country = rng.choice(countries)
            session_duration = rng.randint(10, 1800)
            pages_viewed = rng.randint(1, 40)

            writer.writerow(
                [
                    event_time.strftime("%Y-%m-%d %H:%M:%S"),
                    user_id,
                    session_id,
                    event_type,
                    product_id,
                    price,
                    currency,
                    device_type,
                    browser,
                    referrer,
                    country,
                    session_duration,
                    pages_viewed,
                    month,
                ]
            )


def iter_months(dataset_cfg: Dict[str, Any]) -> Iterable[str]:
    """Yield configured dataset months."""
    months = dataset_cfg.get("months", [])
    if not months:
        raise ValueError("No months configured under dataset.months")
    return months


def main() -> None:
    """Run the download or synthetic data generation step."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    config = load_config(args.config)
    paths = resolve_paths(config, args.config)
    dataset_cfg = config.get("dataset", {})

    raw_dir = paths["data_raw"]
    ensure_dirs(raw_dir)

    url_template = dataset_cfg.get("url_template", "")
    filename_template = (
        dataset_cfg.get("file_pattern")
        or dataset_cfg.get("raw_filename_template")
        or "clickstream_{month}.csv"
    )

    for index, month in enumerate(iter_months(dataset_cfg)):
        filename = filename_template.format(month=month)
        dest = raw_dir / filename
        if dest.exists():
            LOGGER.info("Skipping existing file: %s", dest)
            continue

        if url_template:
            url = url_template.format(month=month)
            download_file(url, dest)
        else:
            LOGGER.info("Generating synthetic data for %s", month)
            generate_synthetic_csv(dest, month, dataset_cfg, index)


if __name__ == "__main__":
    main()
