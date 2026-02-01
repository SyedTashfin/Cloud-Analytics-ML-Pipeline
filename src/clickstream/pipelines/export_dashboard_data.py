from __future__ import annotations

import argparse
import json
import logging
import math
import shutil
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import Bucketizer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from clickstream.config import (
    ensure_dirs,
    filter_feature_columns,
    load_config,
    resolve_paths,
)
from clickstream.spark import get_spark_session
from clickstream.utils.stage_guard import (
    assert_parquet_readable,
    assert_path_exists_and_nonempty,
)
from clickstream.utils.storage import (
    is_gcs_path,
    path_as_str,
    read_text,
    resolve_child_path,
    write_text,
)

LOGGER = logging.getLogger(__name__)

PREFERRED_DRIVER_FEATURES = {
    "num_carts",
    "num_views",
    "session_duration_seconds",
    "avg_price_viewed",
    "max_price_viewed",
    "total_events",
    "unique_products",
    "unique_categories",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
}

FEATURE_DESCRIPTIONS = {
    "total_events": "Total clickstream events in the session.",
    "num_views": "Number of product views in the session.",
    "num_carts": "Number of add-to-cart actions in the session.",
    "unique_products": "Distinct products interacted with in the session.",
    "unique_categories": "Distinct product categories interacted with.",
    "session_duration_seconds": "Session duration in seconds.",
    "avg_price_viewed": "Average price of viewed products in the session.",
    "max_price_viewed": "Maximum price of viewed products in the session.",
    "hour_of_day": "Session start hour (0-23).",
    "day_of_week": "Session start day of week (1=Sun).",
    "is_weekend": "Indicator that the session started on a weekend.",
}


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Export aggregated datasets for the dashboard."
    )
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
        "--reports-root",
        default=None,
        help="Override reports root directory.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional start date filter (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional end date filter (YYYY-MM-DD).",
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


def parse_date(value: Optional[str]) -> Optional[date]:
    """Parse an ISO date string into a date object."""
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def apply_date_filter(
    df: DataFrame, date_col: str, start: Optional[date], end: Optional[date]
) -> DataFrame:
    """Apply an optional date filter on a date column."""
    if start:
        df = df.filter(F.col(date_col) >= F.lit(start))
    if end:
        df = df.filter(F.col(date_col) <= F.lit(end))
    return df


def ensure_event_date(df: DataFrame) -> DataFrame:
    """Ensure event_date exists and is typed as date."""
    if "event_date" in df.columns:
        return df.withColumn("event_date", F.col("event_date").cast("date"))
    return df.withColumn("event_date", F.to_date("event_time"))


def write_parquet_small(df: DataFrame, output_dir: Path, partitions: int = 1) -> None:
    """Write a small parquet dataset with predictable settings."""
    (
        df.coalesce(partitions)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(path_as_str(output_dir))
    )


def normalize_metrics(metrics: Dict[str, object]) -> Dict[str, object]:
    """Normalize metric values to floats where possible."""
    normalized: Dict[str, object] = {}
    for key, value in metrics.items():
        if isinstance(value, (int, float)):
            normalized[key] = float(value)
            continue
        try:
            normalized[key] = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            normalized[key] = value
    return normalized


def safe_int(value: Optional[float]) -> int:
    """Convert a numeric value to int safely."""
    if value is None:
        return 0
    return int(value)


def safe_float(value: Optional[float]) -> float:
    """Convert a numeric value to float safely."""
    if value is None:
        return 0.0
    return float(value)


def format_date(value: Optional[date]) -> Optional[str]:
    """Format a date value as an ISO string."""
    if value is None:
        return None
    return value.isoformat()


def clean_splits(values: Iterable[float]) -> List[float]:
    """Prepare bucket splits for Bucketizer."""
    cleaned = sorted(
        {
            float(value)
            for value in values
            if value is not None and math.isfinite(float(value))
        }
    )
    if not cleaned:
        return []
    if len(cleaned) == 1:
        value = cleaned[0]
        epsilon = max(1.0, abs(value) * 0.1)
        return [value - epsilon, value, value + epsilon]
    if len(cleaned) == 2:
        low, high = cleaned
        midpoint = (low + high) / 2.0
        if midpoint == low or midpoint == high:
            epsilon = max(1.0, abs(low) * 0.1)
            return [low - epsilon, low, high + epsilon]
        return [low, midpoint, high]
    return cleaned


def describe_feature(feature: str) -> str:
    """Return a human-friendly description for a feature."""
    return FEATURE_DESCRIPTIONS.get(feature, "Derived session feature.")


def prioritize_features(
    pairs: List[tuple[str, float]],
    top_n: int,
) -> List[tuple[str, float]]:
    """Prefer interpretable features while preserving weight ordering."""
    preferred = [pair for pair in pairs if pair[0] in PREFERRED_DRIVER_FEATURES]
    others = [pair for pair in pairs if pair[0] not in PREFERRED_DRIVER_FEATURES]
    ordered = preferred + others
    return ordered[:top_n]


def build_driver_records(pairs: List[tuple[str, float]], top_n: int) -> List[dict]:
    """Build driver records with descriptions."""
    records = []
    for feature, weight in prioritize_features(pairs, top_n):
        records.append(
            {
                "feature": feature,
                "weight": round(float(weight), 6),
                "description": describe_feature(feature),
            }
        )
    return records


def build_histogram(
    df: DataFrame,
    column: str,
    date_col: str,
    bins: int,
) -> Optional[DataFrame]:
    """Build a binned histogram by date for a numeric column."""
    base = df.select(column, date_col).where(F.col(column).isNotNull())
    if base.limit(1).count() == 0:
        return None

    quantiles = base.approxQuantile(column, [i / bins for i in range(bins + 1)], 0.01)
    splits = clean_splits(quantiles)
    if len(splits) < 2:
        return None

    bucketizer = Bucketizer(
        splits=splits,
        inputCol=column,
        outputCol="bucket",
        handleInvalid="skip",
    )
    binned = bucketizer.transform(base)
    counts = binned.groupBy(date_col, "bucket").count()

    ranges = [
        (index, float(splits[index]), float(splits[index + 1]))
        for index in range(len(splits) - 1)
    ]
    ranges_df = df.sparkSession.createDataFrame(
        ranges, schema=["bucket", "bin_start", "bin_end"]
    )

    return (
        counts.join(ranges_df, on="bucket", how="left")
        .withColumn("metric", F.lit(column))
        .select("metric", date_col, "bin_start", "bin_end", "count")
    )


def empty_propensity_daily(spark) -> DataFrame:
    """Create an empty daily propensity DataFrame."""
    return spark.createDataFrame(
        [],
        schema=T.StructType(
            [
                T.StructField("date", T.DateType(), True),
                T.StructField("mean_pred_prob", T.DoubleType(), True),
                T.StructField("actual_session_conversion_rate", T.DoubleType(), True),
                T.StructField("sessions", T.LongType(), True),
                T.StructField("purchases", T.LongType(), True),
            ]
        ),
    )


def empty_propensity_hist(spark) -> DataFrame:
    """Create an empty propensity histogram DataFrame."""
    return spark.createDataFrame(
        [],
        schema=T.StructType(
            [
                T.StructField("bucket_low", T.DoubleType(), True),
                T.StructField("bucket_high", T.DoubleType(), True),
                T.StructField("probability_bin", T.StringType(), True),
                T.StructField("sessions_count", T.LongType(), True),
            ]
        ),
    )


def empty_propensity_threshold(spark) -> DataFrame:
    """Create an empty propensity threshold DataFrame."""
    return spark.createDataFrame(
        [],
        schema=T.StructType(
            [
                T.StructField("threshold", T.DoubleType(), True),
                T.StructField("pct_sessions_above", T.DoubleType(), True),
                T.StructField("precision_at_threshold", T.DoubleType(), True),
                T.StructField("recall_at_threshold", T.DoubleType(), True),
            ]
        ),
    )


def empty_calibration_bins(spark) -> DataFrame:
    """Create an empty calibration bins DataFrame."""
    return spark.createDataFrame(
        [],
        schema=T.StructType(
            [
                T.StructField("bin_low", T.DoubleType(), True),
                T.StructField("bin_high", T.DoubleType(), True),
                T.StructField("mean_pred_prob", T.DoubleType(), True),
                T.StructField("actual_rate", T.DoubleType(), True),
                T.StructField("sessions", T.LongType(), True),
            ]
        ),
    )


def empty_threshold_curves(spark) -> DataFrame:
    """Create an empty threshold curve DataFrame."""
    return spark.createDataFrame(
        [],
        schema=T.StructType(
            [
                T.StructField("threshold", T.DoubleType(), True),
                T.StructField("precision", T.DoubleType(), True),
                T.StructField("recall", T.DoubleType(), True),
                T.StructField("f1", T.DoubleType(), True),
                T.StructField("pct_flagged", T.DoubleType(), True),
            ]
        ),
    )


def compute_threshold_metrics(
    scored_df: DataFrame, label_col: str, pred_col: str, threshold: float
) -> Dict[str, float]:
    """Compute threshold metrics for a single threshold."""
    agg = (
        scored_df.agg(
            F.count("*").alias("sessions"),
            F.sum(F.when(F.col(pred_col) >= threshold, 1).otherwise(0)).alias("above"),
            F.sum(
                F.when(
                    (F.col(pred_col) >= threshold) & (F.col(label_col) == 1), 1
                ).otherwise(0)
            ).alias("tp"),
            F.sum(
                F.when(
                    (F.col(pred_col) >= threshold) & (F.col(label_col) == 0), 1
                ).otherwise(0)
            ).alias("fp"),
            F.sum(
                F.when(
                    (F.col(pred_col) < threshold) & (F.col(label_col) == 1), 1
                ).otherwise(0)
            ).alias("fn"),
        ).collect()[0]
    )

    sessions = float(agg["sessions"] or 0)
    above = float(agg["above"] or 0)
    tp = float(agg["tp"] or 0)
    fp = float(agg["fp"] or 0)
    fn = float(agg["fn"] or 0)

    pct_sessions_above = above / sessions if sessions else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0

    return {
        "threshold": float(threshold),
        "pct_sessions_above": round(pct_sessions_above, 6),
        "precision_at_threshold": round(precision, 6),
        "recall_at_threshold": round(recall, 6),
    }


def extract_model_drivers(
    model: LogisticRegressionModel, feature_names: List[str], top_n: int = 8
) -> Dict[str, object]:
    """Extract top positive and negative model coefficients."""
    coefficients = list(model.coefficients)
    pairs = list(zip(feature_names, coefficients))
    positives = sorted([p for p in pairs if p[1] > 0], key=lambda x: x[1], reverse=True)
    negatives = sorted([p for p in pairs if p[1] < 0], key=lambda x: x[1])

    return {
        "source": "model_coefficients",
        "score_label": "weight",
        "positive": build_driver_records(positives, top_n),
        "negative": build_driver_records(negatives, top_n),
    }


def extract_correlation_drivers(
    df: DataFrame, feature_names: List[str], label_col: str, top_n: int = 8
) -> Dict[str, object]:
    """Extract proxy drivers using correlations with the label."""
    correlations = []
    for feature in feature_names:
        value = df.stat.corr(feature, label_col)
        if value is None or math.isnan(float(value)):
            value = 0.0
        correlations.append((feature, float(value)))

    positives = sorted(correlations, key=lambda x: x[1], reverse=True)
    negatives = sorted(correlations, key=lambda x: x[1])

    return {
        "source": "feature_correlation",
        "score_label": "correlation",
        "positive": build_driver_records(positives, top_n),
        "negative": build_driver_records(negatives, top_n),
    }


def main() -> None:
    """Export dashboard aggregates from processed clickstream data."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    paths = resolve_paths(config, args.config)
    dataset_cfg = config.get("dataset", {})
    features_cfg = config.get("features", {})
    eval_cfg = config.get("evaluation", {})
    model_cfg = config.get("model", {})

    processed_root = resolve_override(config_path.parent, args.processed_root)
    if processed_root is None:
        processed_root = paths["data_processed"]

    reports_root = resolve_override(config_path.parent, args.reports_root)
    if reports_root is None:
        reports_root = paths["reports"]

    events_dir = resolve_child_path(
        processed_root, dataset_cfg.get("events_dir", "events_parquet")
    )
    features_dir = resolve_child_path(
        processed_root, dataset_cfg.get("session_features_dir", "session_features")
    )
    model_dir = paths["models"] / model_cfg.get("name", "logreg")
    metrics_path = reports_root / "metrics.json"
    dashboard_dir = reports_root / "dashboard"

    ensure_dirs(reports_root)
    spark = get_spark_session(config)
    assert_path_exists_and_nonempty(
        metrics_path,
        "Missing reports/metrics.json. Run `make evaluate`.",
        spark=spark,
    )

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if start_date and end_date and start_date > end_date:
        raise ValueError("start-date must be before end-date")

    overwrite = should_overwrite(config, args.force)
    prepare_output_dir(dashboard_dir, overwrite)

    assert_parquet_readable(
        spark,
        events_dir,
        "Events parquet missing or empty. Run `make to_parquet`.",
    )
    assert_parquet_readable(
        spark,
        features_dir,
        "Session features missing or empty. Run `make build_features`.",
    )

    events_df = spark.read.parquet(path_as_str(events_dir))
    events_df = ensure_event_date(events_df).filter(
        F.col("event_time").isNotNull() & F.col("user_session").isNotNull()
    )
    events_df = apply_date_filter(events_df, "event_date", start_date, end_date)

    event_types = dataset_cfg.get("event_types", [])
    if event_types:
        events_df = events_df.filter(F.col("event_type").isin(event_types))

    purchases_df = events_df.filter(F.col("event_type") == "purchase")

    events_agg = events_df.agg(
        F.count("*").alias("total_events"),
        F.countDistinct("user_id").alias("total_users"),
        F.min("event_date").alias("min_event_date"),
        F.max("event_date").alias("max_event_date"),
    ).collect()[0]

    purchase_agg = purchases_df.agg(
        F.count("*").alias("total_purchases"),
        F.sum("price").alias("total_revenue"),
        F.avg("price").alias("avg_purchase_price"),
        F.min("price").alias("min_purchase_price"),
        F.max("price").alias("max_purchase_price"),
    ).collect()[0]

    session_features = spark.read.parquet(path_as_str(features_dir))
    session_features_base = (
        session_features.drop("session_date")
        if "session_date" in session_features.columns
        else session_features
    )
    session_dates = events_df.groupBy("user_session").agg(
        F.min("event_time").alias("session_start")
    )
    session_dates = session_dates.withColumn(
        "session_date", F.to_date("session_start")
    )

    sessions_df = session_features_base.join(
        session_dates, on="user_session", how="inner"
    )
    if "day_of_week" not in sessions_df.columns:
        sessions_df = sessions_df.withColumn(
            "day_of_week", F.dayofweek("session_start")
        )
    sessions_df = apply_date_filter(sessions_df, "session_date", start_date, end_date)

    numeric_features = features_cfg.get("numeric", [])
    label_col = features_cfg.get("label", "label")
    features_col = features_cfg.get("features_col", "features")
    leakage_columns = config.get("leakage_columns", [])
    numeric_features, removed_features = filter_feature_columns(
        numeric_features, label_col, leakage_columns
    )
    if removed_features:
        LOGGER.warning("Dropped leakage-prone features: %s", removed_features)
    threshold_value = float(eval_cfg.get("threshold", 0.5))

    propensity_daily = empty_propensity_daily(spark)
    propensity_hist = empty_propensity_hist(spark)
    propensity_threshold = empty_propensity_threshold(spark)
    calibration_bins = empty_calibration_bins(spark)
    threshold_curves = empty_threshold_curves(spark)
    propensity_drivers: Dict[str, object] = {
        "source": "unavailable",
        "score_label": "weight",
        "positive": [],
        "negative": [],
    }
    propensity_kpis: Dict[str, object] = {
        "available": False,
        "predicted_conversion_rate": None,
        "actual_session_conversion_rate": None,
        "calibration_gap": None,
        "high_propensity_share": None,
        "threshold": threshold_value,
    }

    if numeric_features and label_col in session_features.columns:
        available_features = [
            feature for feature in numeric_features if feature in session_features.columns
        ]
        missing_features = sorted(set(numeric_features) - set(available_features))
        if missing_features:
            LOGGER.warning("Missing configured features for scoring: %s", missing_features)
        if not available_features:
            LOGGER.warning("No usable features available for propensity scoring.")
            available_features = []

        fill_values = {col_name: 0 for col_name in available_features}
        drivers_base = (
            session_features.select(*available_features, label_col)
            .fillna(fill_values)
            .withColumn(label_col, F.col(label_col).cast("int"))
        )
        model = None
        try:
            assert_path_exists_and_nonempty(
                model_dir, "Model artifacts missing. Run `make train`.", spark=spark
            )
            model = LogisticRegressionModel.load(path_as_str(model_dir))
            propensity_drivers = extract_model_drivers(model, available_features)
        except Exception as exc:
            LOGGER.warning("Model drivers unavailable: %s", exc)
            propensity_drivers = extract_correlation_drivers(
                drivers_base, available_features, label_col
            )

        if model is not None and available_features:
            sessions_in_range = sessions_df.select("user_session", "session_date")
            base_features = (
                session_features.drop("session_date")
                if "session_date" in session_features.columns
                else session_features
            )
            scoring_df = base_features.join(
                sessions_in_range, on="user_session", how="inner"
            ).fillna(fill_values)

            assembler = VectorAssembler(
                inputCols=available_features, outputCol=features_col
            )
            scoring_df = assembler.transform(scoring_df)
            scoring_df = scoring_df.withColumn(label_col, F.col(label_col).cast("int"))

            scored = model.transform(
                scoring_df.select("session_date", label_col, features_col)
            )
            pred_prob = vector_to_array(F.col("probability")).getItem(1)
            scored = scored.select(
                "session_date",
                F.col(label_col).alias("label"),
                pred_prob.alias("pred_prob"),
            )

            propensity_daily = scored.groupBy("session_date").agg(
                F.count("*").alias("sessions"),
                F.sum("label").alias("purchases"),
                F.avg("pred_prob").alias("mean_pred_prob"),
            )
            propensity_daily = propensity_daily.withColumn(
                "actual_session_conversion_rate",
                F.when(F.col("sessions") > 0, F.col("purchases") / F.col("sessions"))
                .otherwise(0.0),
            ).withColumnRenamed("session_date", "date")

            scored_summary = scored.agg(
                F.count("*").alias("sessions"),
                F.sum("label").alias("purchases"),
                F.avg("pred_prob").alias("mean_pred_prob"),
                F.sum(F.when(F.col("pred_prob") >= threshold_value, 1).otherwise(0)).alias(
                    "high_propensity_sessions"
                ),
            ).collect()[0]
            sessions_count = float(scored_summary["sessions"] or 0)
            purchases_count = float(scored_summary["purchases"] or 0)
            mean_pred = float(scored_summary["mean_pred_prob"] or 0)
            actual_conv = purchases_count / sessions_count if sessions_count else 0.0
            high_propensity = (
                float(scored_summary["high_propensity_sessions"] or 0) / sessions_count
                if sessions_count
                else 0.0
            )
            propensity_kpis = {
                "available": True,
                "predicted_conversion_rate": round(mean_pred, 6),
                "actual_session_conversion_rate": round(actual_conv, 6),
                "calibration_gap": round(mean_pred - actual_conv, 6),
                "high_propensity_share": round(high_propensity, 6),
                "threshold": round(float(threshold_value), 6),
            }

            splits = [round(index / 10, 2) for index in range(11)]
            bucketizer = Bucketizer(
                splits=splits,
                inputCol="pred_prob",
                outputCol="bucket",
                handleInvalid="skip",
            )
            binned = bucketizer.transform(scored)
            counts = binned.groupBy("bucket").count()
            ranges = [
                (index, float(splits[index]), float(splits[index + 1]))
                for index in range(len(splits) - 1)
            ]
            ranges_df = spark.createDataFrame(
                ranges, schema=["bucket", "bucket_low", "bucket_high"]
            )
            propensity_hist = counts.join(ranges_df, on="bucket", how="left").withColumn(
                "probability_bin",
                F.concat_ws(
                    "-",
                    F.format_number(F.col("bucket_low"), 1),
                    F.format_number(F.col("bucket_high"), 1),
                ),
            )
            propensity_hist = propensity_hist.select(
                "bucket_low",
                "bucket_high",
                "probability_bin",
                F.col("count").alias("sessions_count"),
            ).orderBy("bucket_low")

            calibration_bins = (
                binned.groupBy("bucket")
                .agg(
                    F.count("*").alias("sessions"),
                    F.avg("pred_prob").alias("mean_pred_prob"),
                    F.avg("label").alias("actual_rate"),
                )
                .join(ranges_df, on="bucket", how="left")
                .select(
                    F.col("bucket_low").alias("bin_low"),
                    F.col("bucket_high").alias("bin_high"),
                    "mean_pred_prob",
                    "actual_rate",
                    "sessions",
                )
                .orderBy("bin_low")
            )

            threshold_metrics = compute_threshold_metrics(
                scored, "label", "pred_prob", threshold_value
            )
            propensity_threshold = spark.createDataFrame([threshold_metrics])

            threshold_splits = [round(index / 100, 2) for index in range(101)]
            threshold_bucketizer = Bucketizer(
                splits=threshold_splits,
                inputCol="pred_prob",
                outputCol="threshold_bucket",
                handleInvalid="skip",
            )
            threshold_binned = threshold_bucketizer.transform(scored)
            threshold_counts = (
                threshold_binned.groupBy("threshold_bucket")
                .agg(
                    F.count("*").alias("sessions"),
                    F.sum("label").alias("positives"),
                )
                .withColumnRenamed("threshold_bucket", "bucket")
            )
            threshold_ranges = [
                (index, float(threshold_splits[index]), float(threshold_splits[index + 1]))
                for index in range(len(threshold_splits) - 1)
            ]
            threshold_ranges_df = spark.createDataFrame(
                threshold_ranges, schema=["bucket", "bucket_low", "bucket_high"]
            )
            threshold_bins = (
                threshold_counts.join(threshold_ranges_df, on="bucket", how="left")
                .select("bucket_low", "sessions", "positives")
                .orderBy("bucket_low")
            )
            threshold_rows = threshold_bins.collect()
            sessions_total = sum(float(row["sessions"] or 0) for row in threshold_rows)
            positives_total = sum(float(row["positives"] or 0) for row in threshold_rows)

            sessions_by_bin = [float(row["sessions"] or 0) for row in threshold_rows]
            positives_by_bin = [float(row["positives"] or 0) for row in threshold_rows]
            bucket_lows = [float(row["bucket_low"] or 0) for row in threshold_rows]

            cumulative_sessions = []
            cumulative_positives = []
            running_sessions = 0.0
            running_positives = 0.0
            for sessions, positives in zip(
                reversed(sessions_by_bin), reversed(positives_by_bin)
            ):
                running_sessions += sessions
                running_positives += positives
                cumulative_sessions.append(running_sessions)
                cumulative_positives.append(running_positives)

            cumulative_sessions = list(reversed(cumulative_sessions))
            cumulative_positives = list(reversed(cumulative_positives))

            threshold_records = []
            for idx, threshold in enumerate(bucket_lows):
                flagged = cumulative_sessions[idx]
                tp = cumulative_positives[idx]
                fp = flagged - tp
                fn = positives_total - tp
                precision = tp / (tp + fp) if (tp + fp) else 0.0
                recall = tp / positives_total if positives_total else 0.0
                f1 = (
                    2 * precision * recall / (precision + recall)
                    if (precision + recall)
                    else 0.0
                )
                pct_flagged = flagged / sessions_total if sessions_total else 0.0
                threshold_records.append(
                    {
                        "threshold": round(float(threshold), 6),
                        "precision": round(float(precision), 6),
                        "recall": round(float(recall), 6),
                        "f1": round(float(f1), 6),
                        "pct_flagged": round(float(pct_flagged), 6),
                    }
                )

            threshold_records.append(
                {
                    "threshold": 1.0,
                    "precision": 0.0,
                    "recall": 0.0,
                    "f1": 0.0,
                    "pct_flagged": 0.0,
                }
            )
            threshold_curves = spark.createDataFrame(threshold_records)

    session_agg = sessions_df.agg(
        F.count("*").alias("total_sessions"),
        F.sum("purchase_in_session").alias("purchase_sessions"),
        F.avg("session_duration_seconds").alias("avg_session_duration_seconds"),
        F.avg("total_events").alias("avg_events_per_session"),
    ).collect()[0]

    total_sessions = safe_int(session_agg["total_sessions"])
    purchase_sessions = safe_int(session_agg["purchase_sessions"])
    conversion_rate = purchase_sessions / total_sessions if total_sessions else 0.0

    total_purchases = safe_int(purchase_agg["total_purchases"])
    total_revenue = safe_float(purchase_agg["total_revenue"])
    avg_order_value = total_revenue / total_purchases if total_purchases else 0.0

    kpis = {
        "total_events": safe_int(events_agg["total_events"]),
        "total_users": safe_int(events_agg["total_users"]),
        "total_sessions": total_sessions,
        "total_purchases": total_purchases,
        "total_revenue": round(total_revenue, 2),
        "avg_order_value": round(avg_order_value, 2),
        "conversion_rate": round(float(conversion_rate), 6),
        "avg_session_duration_seconds": round(
            safe_float(session_agg["avg_session_duration_seconds"]), 2
        ),
        "avg_events_per_session": round(
            safe_float(session_agg["avg_events_per_session"]), 2
        ),
        "min_event_date": format_date(events_agg["min_event_date"]),
        "max_event_date": format_date(events_agg["max_event_date"]),
        "min_purchase_price": round(
            safe_float(purchase_agg["min_purchase_price"]), 2
        ),
        "max_purchase_price": round(
            safe_float(purchase_agg["max_purchase_price"]), 2
        ),
        "currency": dataset_cfg.get("currency", "USD"),
    }

    funnel_daily = events_df.groupBy("event_date").agg(
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
            "purchases"
        ),
        F.countDistinct("user_session").alias("sessions"),
    )

    purchases_by_hour = purchases_df.withColumn("hour", F.hour("event_time")).groupBy(
        "event_date", "hour"
    ).agg(
        F.count("*").alias("purchase_count"),
        F.sum("price").alias("revenue"),
        F.avg("price").alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
    )

    conversion_by_weekday = sessions_df.groupBy(
        "session_date", "day_of_week"
    ).agg(
        F.count("*").alias("sessions"),
        F.sum("purchase_in_session").alias("purchases"),
    )
    conversion_by_weekday = conversion_by_weekday.withColumn(
        "conversion_rate",
        F.when(F.col("sessions") > 0, F.col("purchases") / F.col("sessions")).otherwise(
            0.0
        ),
    )

    categories_df = purchases_df.withColumn(
        "category",
        F.coalesce(
            F.col("category_code"), F.col("category_id").cast("string")
        ),
    )
    categories_df = categories_df.withColumn(
        "category",
        F.when(
            F.col("category").isNull() | (F.col("category") == ""),
            F.lit("unknown"),
        ).otherwise(F.col("category")),
    )

    top_categories = categories_df.groupBy("event_date", "category").agg(
        F.count("*").alias("purchase_count"),
        F.sum("price").alias("revenue"),
        F.avg("price").alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
    )

    brands_df = purchases_df.withColumn(
        "brand",
        F.when(
            F.col("brand").isNull() | (F.col("brand") == ""),
            F.lit("unknown"),
        ).otherwise(F.col("brand")),
    )
    top_brands = brands_df.groupBy("event_date", "brand").agg(
        F.count("*").alias("purchase_count"),
        F.sum("price").alias("revenue"),
        F.avg("price").alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
    )

    histogram_metrics = [
        "session_duration_seconds",
        "total_events",
        "num_views",
        "num_carts",
        "num_purchases",
        "avg_price_viewed",
        "max_price_viewed",
    ]
    available_metrics = [
        metric for metric in histogram_metrics if metric in sessions_df.columns
    ]

    histograms: List[DataFrame] = []
    for metric in available_metrics:
        histogram = build_histogram(sessions_df, metric, "session_date", bins=10)
        if histogram is not None:
            histograms.append(histogram)

    if histograms:
        session_distributions = histograms[0]
        for histogram in histograms[1:]:
            session_distributions = session_distributions.unionByName(histogram)
    else:
        session_distributions = spark.createDataFrame(
            [],
            schema=T.StructType(
                [
                    T.StructField("metric", T.StringType(), True),
                    T.StructField("session_date", T.DateType(), True),
                    T.StructField("bin_start", T.DoubleType(), True),
                    T.StructField("bin_end", T.DoubleType(), True),
                    T.StructField("count", T.LongType(), True),
                ]
            ),
        )

    model_metrics = normalize_metrics(
        json.loads(read_text(metrics_path, spark=spark))
    )

    write_text(dashboard_dir / "kpis.json", json.dumps(kpis, indent=2), spark=spark)
    write_text(
        dashboard_dir / "model_metrics.json",
        json.dumps(model_metrics, indent=2),
        spark=spark,
    )
    write_text(
        dashboard_dir / "propensity_drivers.json",
        json.dumps(propensity_drivers, indent=2),
        spark=spark,
    )
    write_text(
        dashboard_dir / "propensity_kpis.json",
        json.dumps(propensity_kpis, indent=2),
        spark=spark,
    )

    LOGGER.info("Writing dashboard aggregates to %s", dashboard_dir)
    write_parquet_small(funnel_daily, dashboard_dir / "funnel_daily.parquet")
    write_parquet_small(purchases_by_hour, dashboard_dir / "purchases_by_hour.parquet")
    write_parquet_small(
        conversion_by_weekday, dashboard_dir / "conversion_by_weekday.parquet"
    )
    write_parquet_small(top_categories, dashboard_dir / "top_categories.parquet")
    write_parquet_small(top_brands, dashboard_dir / "top_brands.parquet")
    write_parquet_small(
        session_distributions, dashboard_dir / "session_distributions.parquet"
    )
    write_parquet_small(propensity_daily, dashboard_dir / "propensity_daily.parquet")
    write_parquet_small(propensity_hist, dashboard_dir / "propensity_hist.parquet")
    write_parquet_small(
        propensity_threshold, dashboard_dir / "propensity_threshold.parquet"
    )
    write_parquet_small(
        calibration_bins, dashboard_dir / "calibration_bins.parquet"
    )
    write_parquet_small(
        threshold_curves, dashboard_dir / "threshold_curves.parquet"
    )

    spark.stop()


if __name__ == "__main__":
    main()
