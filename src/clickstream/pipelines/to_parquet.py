from __future__ import annotations

import argparse
import json
import logging
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from clickstream.config import ensure_dirs, load_config, resolve_paths
from clickstream.spark import get_spark_session
from clickstream.utils.storage import (
    is_gcs_path,
    path_as_str,
    resolve_child_path,
    write_text,
)

LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]

SCHEMA = T.StructType(
    [
        T.StructField("event_time", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("product_id", T.LongType(), True),
        T.StructField("category_id", T.LongType(), True),
        T.StructField("category_code", T.StringType(), True),
        T.StructField("brand", T.StringType(), True),
        T.StructField("price", T.DoubleType(), True),
        T.StructField("user_id", T.LongType(), True),
        T.StructField("user_session", T.StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Convert raw CSV to parquet.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing outputs.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for smoke runs.",
    )
    parser.add_argument(
        "--processed-root",
        default=None,
        help="Override processed data root directory.",
    )
    parser.add_argument(
        "--reports-suffix",
        default="",
        help="Suffix for the ingestion summary filename.",
    )
    parser.add_argument(
        "--max-days-per-run",
        type=int,
        default=None,
        help="Maximum number of days per chunk when writing parquet.",
    )
    return parser.parse_args()


def gather_raw_files(
    raw_dir: Path, months: List[str], filename_template: str
) -> List[str]:
    """Collect raw CSV files for configured months."""
    files = []
    for month in months:
        path = raw_dir / filename_template.format(month=month)
        if is_gcs_path(path):
            files.append(path_as_str(path))
        elif path.exists():
            files.append(str(path))
    return files


def read_raw(spark, files: List[str]) -> DataFrame:
    """Read raw CSV files into a Spark DataFrame."""
    return (
        spark.read.schema(SCHEMA)
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(files)
    )


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


def get_max_days(config: dict, cli_value: Optional[int]) -> int:
    """Resolve max days per run from CLI or config defaults."""
    if cli_value is not None:
        return cli_value
    return int(config.get("processing", {}).get("max_days_per_run", 7))


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

    if overwrite:
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir.parent.mkdir(parents=True, exist_ok=True)


def format_timestamp(value) -> Optional[str]:
    """Format a datetime timestamp for JSON output."""
    if value is None:
        return None
    return value.isoformat(sep=" ")


def iter_date_ranges(
    start_date: date, end_date: date, max_days: int
) -> Iterable[Tuple[date, date]]:
    """Yield inclusive date ranges up to a maximum size."""
    current = start_date
    step = timedelta(days=max_days - 1)
    while current <= end_date:
        chunk_end = min(current + step, end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def write_parquet(
    df: DataFrame,
    output_dir: Path,
    partitions: int,
    mode: str,
) -> None:
    """Write a parquet chunk with consistent settings."""
    (
        df.repartition(partitions, "event_date")
        .write.mode(mode)
        .option("compression", "snappy")
        .partitionBy("event_date")
        .parquet(path_as_str(output_dir))
    )


def main() -> None:
    """Convert raw CSV data into partitioned parquet."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    paths = resolve_paths(config, args.config)
    dataset_cfg = config.get("dataset", {})

    raw_dir = paths["data_raw"]
    raw_override = dataset_cfg.get("raw_dir")
    if raw_override:
        raw_dir = resolve_child_path(config_path.parent, raw_override)
    processed_root = resolve_override(config_path.parent, args.processed_root)
    if processed_root is None:
        processed_root = paths["data_processed"]
    reports_dir = paths["reports"]

    events_dir = resolve_child_path(
        processed_root, dataset_cfg.get("events_dir", "events_parquet")
    )
    ensure_dirs(reports_dir)

    months = dataset_cfg.get("months", [])
    if not months:
        raise ValueError("No months configured under dataset.months")

    filename_template = (
        dataset_cfg.get("file_pattern")
        or dataset_cfg.get("raw_filename_template")
        or "clickstream_{month}.csv"
    )
    files = gather_raw_files(raw_dir, months, filename_template)
    if not files:
        raise FileNotFoundError("No raw CSV files found. Run make download_data.")

    overwrite = should_overwrite(config, args.force)
    prepare_output_dir(events_dir, overwrite)

    spark = get_spark_session(config)
    max_days = get_max_days(config, args.max_days_per_run)
    is_local = spark.sparkContext.master.lower().startswith("local")
    chunk_mode = max_days > 0
    if is_local and chunk_mode:
        LOGGER.info("Running in LOCAL CHUNK MODE (safe for laptops)")

    df = read_raw(spark, files).select(*REQUIRED_COLUMNS)
    if args.limit:
        df = df.limit(args.limit)

    input_rows = df.count()

    event_time_clean = F.regexp_replace(
        F.trim(F.col("event_time")), r"\s+UTC$", ""
    )
    df = df.withColumn("event_time_clean", event_time_clean)
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("event_time_clean"), "yyyy-MM-dd HH:mm:ss"),
    ).drop("event_time_clean")
    df = df.withColumn("event_date", F.to_date(F.col("event_time")))

    df = df.filter(
        F.col("user_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("event_type").isNotNull()
    )
    df = df.filter((F.col("price") >= 0) | F.col("price").isNull())

    summary_row = (
        df.agg(
            F.count("*").alias("output_rows"),
            F.countDistinct("user_id").alias("distinct_users"),
            F.countDistinct("user_session").alias("distinct_sessions"),
            F.min("event_time").alias("min_event_time"),
            F.max("event_time").alias("max_event_time"),
        ).collect()[0]
    )

    output_rows = int(summary_row["output_rows"])
    if output_rows == 0:
        raise ValueError("No valid rows after cleaning. Check raw CSV files.")

    summary = {
        "input_rows": int(input_rows),
        "output_rows": output_rows,
        "distinct_users": int(summary_row["distinct_users"]),
        "distinct_sessions": int(summary_row["distinct_sessions"]),
        "min_event_time": format_timestamp(summary_row["min_event_time"]),
        "max_event_time": format_timestamp(summary_row["max_event_time"]),
    }

    min_time = summary_row["min_event_time"]
    max_time = summary_row["max_event_time"]
    if min_time is None or max_time is None:
        raise ValueError("Missing event_time bounds after cleaning.")

    partitions = min(64, spark.sparkContext.defaultParallelism)

    if chunk_mode:
        start_date = min_time.date()
        end_date = max_time.date()
        first_chunk = True
        for chunk_start, chunk_end in iter_date_ranges(
            start_date, end_date, max_days
        ):
            LOGGER.info(
                "Writing chunk %s to %s", chunk_start.isoformat(), chunk_end.isoformat()
            )
            chunk_df = df.filter(
                (F.col("event_date") >= F.lit(chunk_start))
                & (F.col("event_date") <= F.lit(chunk_end))
            )
            mode = "overwrite" if overwrite and first_chunk else "append"
            write_parquet(chunk_df, events_dir, partitions, mode)
            first_chunk = False
    else:
        mode = "overwrite" if overwrite else "errorifexists"
        LOGGER.info("Writing parquet to %s", events_dir)
        write_parquet(df, events_dir, partitions, mode)

    summary_suffix = args.reports_suffix
    summary_path = reports_dir / f"ingestion_summary{summary_suffix}.json"
    write_text(summary_path, json.dumps(summary, indent=2), spark=spark)

    spark.stop()


if __name__ == "__main__":
    main()
