from __future__ import annotations

import argparse
import csv
import json
import logging
import io
from pathlib import Path

from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum as spark_sum, when

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
from clickstream.utils.storage import path_as_str, resolve_child_path, write_text

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Evaluate a trained model.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    return parser.parse_args()


def write_metrics_csv(path: Path, metrics: dict, spark) -> None:
    """Write metrics to a CSV file."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["metric", "value"])
    for key, value in metrics.items():
        writer.writerow([key, value])
    write_text(path, buffer.getvalue(), spark=spark)


def get_date_bounds(df, time_col: str) -> tuple[str | None, str | None]:
    """Get min and max dates for a time column."""
    if time_col not in df.columns:
        return None, None
    row = df.agg(
        F.min(F.col(time_col)).alias("start"),
        F.max(F.col(time_col)).alias("end"),
    ).collect()[0]
    start = row["start"]
    end = row["end"]
    return (start.isoformat() if start else None, end.isoformat() if end else None)


def main() -> None:
    """Evaluate model performance on the test set."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    config = load_config(args.config)
    paths = resolve_paths(config, args.config)
    dataset_cfg = config.get("dataset", {})
    features_cfg = config.get("features", {})
    eval_cfg = config.get("evaluation", {})
    split_cfg = config.get("split", {})
    leakage_columns = config.get("leakage_columns", [])
    model_cfg = config.get("model", {})

    processed_dir = paths["data_processed"]
    test_dir = resolve_child_path(processed_dir, dataset_cfg.get("test_dir", "test"))
    train_dir = resolve_child_path(processed_dir, dataset_cfg.get("train_dir", "train"))
    model_dir = paths["models"] / model_cfg.get("name", "logreg")

    reports_dir = paths["reports"]
    ensure_dirs(reports_dir)

    metrics_json = reports_dir / "metrics.json"
    metrics_csv = reports_dir / "metrics.csv"

    spark = get_spark_session(config)
    assert_parquet_readable(
        spark,
        test_dir,
        "Test data missing or empty. Run `make train`.",
    )
    assert_parquet_readable(
        spark,
        train_dir,
        "Train data missing or empty. Run `make train`.",
    )
    assert_path_exists_and_nonempty(
        model_dir,
        "Model artifacts missing. Run `make train`.",
        spark=spark,
    )

    test_df = spark.read.parquet(path_as_str(test_dir))
    train_df = spark.read.parquet(path_as_str(train_dir))
    model = LogisticRegressionModel.load(path_as_str(model_dir))

    label_col = features_cfg.get("label", "label")
    threshold = float(eval_cfg.get("threshold", 0.5))
    time_col = split_cfg.get("time_column", "session_date")
    split_method = split_cfg.get("method", "time")

    numeric_features = features_cfg.get("numeric", [])
    filtered_features, _ = filter_feature_columns(
        numeric_features, label_col, leakage_columns
    )

    predictions = model.transform(test_df)
    p1 = vector_to_array(col("probability")).getItem(1)
    predictions = predictions.withColumn(
        "prediction_label", (p1 >= threshold).cast("int")
    )
    pred_label = col("prediction_label")

    evaluator = BinaryClassificationEvaluator(labelCol=label_col)
    auc = evaluator.evaluate(predictions)

    agg = (
        predictions.agg(
            count("*").alias("total"),
            spark_sum(when(col(label_col) == 1, 1).otherwise(0)).alias("positives"),
            spark_sum(
                when((col(label_col) == 1) & (pred_label == 1), 1).otherwise(0)
            ).alias("tp"),
            spark_sum(
                when((col(label_col) == 0) & (pred_label == 1), 1).otherwise(0)
            ).alias("fp"),
            spark_sum(
                when((col(label_col) == 1) & (pred_label == 0), 1).otherwise(0)
            ).alias("fn"),
            spark_sum(
                when((col(label_col) == 0) & (pred_label == 0), 1).otherwise(0)
            ).alias("tn"),
        ).collect()[0]
    )

    total = agg["total"]
    positives = agg["positives"]
    tp = agg["tp"]
    fp = agg["fp"]
    fn = agg["fn"]
    tn = agg["tn"]

    accuracy = (tp + tn) / total if total else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    positive_rate = positives / total if total else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    train_counts = (
        train_df.agg(
            count("*").alias("total"),
            spark_sum(when(col(label_col) == 1, 1).otherwise(0)).alias("positives"),
        ).collect()[0]
    )
    train_total = train_counts["total"]
    train_pos = train_counts["positives"]
    pct_positive_train = train_pos / train_total if train_total else 0.0
    pct_positive_test = positive_rate

    train_start, train_end = get_date_bounds(train_df, time_col)
    test_start, test_end = get_date_bounds(test_df, time_col)

    split_method_value = "random"
    if split_method in {"time", "time-based"} and train_end and test_start:
        split_method_value = "time-based" if train_end <= test_start else "random"

    auc_value = round(float(auc), 6)
    metrics = {
        "split_method": split_method_value,
        "train_start": train_start,
        "train_end": train_end,
        "test_start": test_start,
        "test_end": test_end,
        "pct_positive_train": round(float(pct_positive_train), 6),
        "pct_positive_test": round(float(pct_positive_test), 6),
        "threshold_used": round(float(threshold), 6),
        "label_column": label_col,
        "features_used": filtered_features,
        "auc": auc_value,
        "accuracy": round(float(accuracy), 6),
        "precision": round(float(precision), 6),
        "recall": round(float(recall), 6),
        "f1": round(float(f1), 6),
        "positive_rate": round(float(positive_rate), 6),
        "confusion_matrix": {
            "tn": int(tn),
            "fp": int(fp),
            "fn": int(fn),
            "tp": int(tp),
        },
        "evaluation_set": "test",
        "suspiciously_perfect_metrics": bool(auc_value >= 1.0),
    }

    LOGGER.info("Writing metrics to %s", metrics_json)
    write_text(metrics_json, json.dumps(metrics, indent=2), spark=spark)
    write_metrics_csv(metrics_csv, metrics, spark)

    spark.stop()


if __name__ == "__main__":
    main()
