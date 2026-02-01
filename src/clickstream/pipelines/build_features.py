from __future__ import annotations

import argparse
import json
import logging
import shutil
from pathlib import Path
from typing import Optional

from pyspark.sql import functions as F

from clickstream.config import ensure_dirs, load_config, resolve_paths
from clickstream.spark import get_spark_session
from clickstream.utils.stage_guard import assert_parquet_readable
from clickstream.utils.storage import (
    is_gcs_path,
    path_as_str,
    resolve_child_path,
    write_text,
)

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Build session-level features.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing outputs.",
    )
    parser.add_argument(
        "--processed-root",
        default=None,
        help="Override processed data root directory.",
    )
    parser.add_argument(
        "--reports-suffix",
        default="",
        help="Suffix for the features summary filename.",
    )
    return parser.parse_args()


def resolve_override(base_dir: Path, value: Optional[str]) -> Optional[Path]:
    """Resolve an optional path override relative to the config directory."""
    if value is None:
        return None
    if is_gcs_path(value):
        return Path(value)
    path = Path(value)
    return path if path.is_absolute() else (base_dir / path).resolve()


def should_overwrite(config: dict, force: bool) -> bool:
    """Determine whether outputs should be overwritten."""
    if force:
        return True
    return bool(config.get("processing", {}).get("overwrite", False))


def prepare_output_dir(output_dir: Path, overwrite: bool) -> None:
    """Ensure output directory is safe to write."""
    if is_gcs_path(output_dir):
        return
    if output_dir.exists():
        if not overwrite:
            raise FileExistsError(
                f"Output already exists: {output_dir}. Run with --force to overwrite."
            )
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """Build session-level features from events parquet data."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    paths = resolve_paths(config, args.config)
    dataset_cfg = config.get("dataset", {})

    processed_root = resolve_override(config_path.parent, args.processed_root)
    if processed_root is None:
        processed_root = paths["data_processed"]

    events_dir = resolve_child_path(
        processed_root, dataset_cfg.get("events_dir", "events_parquet")
    )
    features_dir = resolve_child_path(
        processed_root, dataset_cfg.get("session_features_dir", "session_features")
    )
    reports_dir = paths["reports"]

    ensure_dirs(reports_dir)

    spark = get_spark_session(config)
    assert_parquet_readable(
        spark,
        events_dir,
        "Events parquet missing or empty. Run `make to_parquet --force` "
        "or `make rerun_all_force`.",
    )

    df = spark.read.parquet(path_as_str(events_dir)).filter(
        F.col("user_session").isNotNull()
    )

    session_stats = df.groupBy("user_session").agg(
        F.first("user_id", ignorenulls=True).alias("user_id"),
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias(
            "num_views"
        ),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias(
            "num_carts"
        ),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
            "num_purchases"
        ),
        F.countDistinct("product_id").alias("unique_products"),
        F.countDistinct("category_id").alias("unique_categories"),
        F.min("event_time").alias("session_start"),
        F.max("event_time").alias("session_end"),
        F.avg(F.when(F.col("event_type") == "view", F.col("price"))).alias(
            "avg_price_viewed"
        ),
        F.max(F.when(F.col("event_type") == "view", F.col("price"))).alias(
            "max_price_viewed"
        ),
        F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
            "purchase_in_session"
        ),
    )

    session_stats = session_stats.withColumn(
        "session_duration_seconds",
        F.unix_timestamp(F.col("session_end"))
        - F.unix_timestamp(F.col("session_start")),
    )
    session_stats = session_stats.withColumn(
        "session_date", F.to_date(F.col("session_start"))
    )
    session_stats = session_stats.withColumn(
        "hour_of_day", F.hour(F.col("session_start"))
    )
    session_stats = session_stats.withColumn(
        "day_of_week", F.dayofweek(F.col("session_start"))
    )
    session_stats = session_stats.withColumn(
        "is_weekend",
        F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0),
    )

    features_df = session_stats.select(
        "user_session",
        "user_id",
        "total_events",
        "num_views",
        "num_carts",
        "num_purchases",
        "unique_products",
        "unique_categories",
        "session_duration_seconds",
        "session_date",
        "avg_price_viewed",
        "max_price_viewed",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "purchase_in_session",
    )

    overwrite = should_overwrite(config, args.force)
    prepare_output_dir(features_dir, overwrite)

    LOGGER.info("Writing session features to %s", features_dir)
    features_df.write.mode("overwrite").parquet(path_as_str(features_dir))

    summary_row = (
        features_df.agg(
            F.count("*").alias("row_count"),
            F.sum("purchase_in_session").alias("positives"),
        ).collect()[0]
    )

    row_count = int(summary_row["row_count"])
    positives = int(summary_row["positives"] or 0)
    positive_rate = positives / row_count if row_count else 0.0

    summary = {
        "row_count": row_count,
        "positive_rate": round(float(positive_rate), 6),
    }

    summary_suffix = args.reports_suffix
    summary_path = reports_dir / f"features_summary{summary_suffix}.json"
    write_text(summary_path, json.dumps(summary, indent=2), spark=spark)

    spark.stop()


if __name__ == "__main__":
    main()
