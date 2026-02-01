from __future__ import annotations

import argparse
import logging

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from clickstream.config import (
    ensure_dirs,
    filter_feature_columns,
    load_config,
    resolve_paths,
)
from clickstream.spark import get_spark_session
from clickstream.utils.stage_guard import assert_parquet_readable
from clickstream.utils.storage import path_as_str, resolve_child_path

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Train a clickstream model.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    return parser.parse_args()


def main() -> None:
    """Train a logistic regression model on session features."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    config = load_config(args.config)
    paths = resolve_paths(config, args.config)
    dataset_cfg = config.get("dataset", {})
    features_cfg = config.get("features", {})
    training_cfg = config.get("training", {})
    model_cfg = config.get("model", {})

    processed_dir = paths["data_processed"]
    features_dir = resolve_child_path(
        processed_dir, dataset_cfg.get("session_features_dir", "session_features")
    )
    train_dir = resolve_child_path(processed_dir, dataset_cfg.get("train_dir", "train"))
    test_dir = resolve_child_path(processed_dir, dataset_cfg.get("test_dir", "test"))
    model_dir = paths["models"] / model_cfg.get("name", "logreg")

    ensure_dirs(processed_dir, train_dir, test_dir, model_dir)

    spark = get_spark_session(config)
    assert_parquet_readable(
        spark,
        features_dir,
        "Session features missing or empty. Run `make build_features --force` "
        "or `make rerun_all_force`.",
    )

    df = spark.read.parquet(path_as_str(features_dir))

    numeric_features = features_cfg.get("numeric", [])
    if not numeric_features:
        raise ValueError("No numeric features configured under features.numeric")

    label_col = features_cfg.get("label", "label")
    features_col = features_cfg.get("features_col", "features")
    leakage_columns = config.get("leakage_columns", [])
    numeric_features, removed = filter_feature_columns(
        numeric_features, label_col, leakage_columns
    )
    if removed:
        LOGGER.warning("Dropped leakage-prone features: %s", removed)

    available_features = [col for col in numeric_features if col in df.columns]
    missing_features = sorted(set(numeric_features) - set(available_features))
    if missing_features:
        LOGGER.warning("Missing configured features: %s", missing_features)
    if not available_features:
        raise ValueError("No usable numeric features remain after leakage filtering.")

    fill_values = {col_name: 0 for col_name in available_features}
    df = df.fillna(fill_values)

    assembler = VectorAssembler(inputCols=available_features, outputCol=features_col)
    df = assembler.transform(df)
    df = df.withColumn(label_col, F.col(label_col).cast("int"))

    split_cfg = config.get("split", {})
    split_method = split_cfg.get("method", "time").lower()
    time_col = split_cfg.get("time_column", "session_date")

    select_cols = [label_col, features_col]
    if time_col in df.columns:
        select_cols.append(time_col)
    df = df.select(*select_cols)

    train_ratio = float(split_cfg.get("train_ratio", 0.8))
    seed = int(split_cfg.get("seed", 42))

    use_time_split = split_method in {"time", "time-based"}
    train_df = None
    test_df = None

    if use_time_split and time_col in df.columns:
        date_rows = (
            df.select(time_col)
            .where(F.col(time_col).isNotNull())
            .distinct()
            .orderBy(time_col)
            .collect()
        )
        dates = [row[time_col] for row in date_rows]
        if len(dates) >= 2:
            cutoff = max(1, min(len(dates) - 1, int(len(dates) * train_ratio)))
            train_dates = dates[:cutoff]
            test_dates = dates[cutoff:]
            train_df = df.filter(F.col(time_col).isin(train_dates))
            test_df = df.filter(F.col(time_col).isin(test_dates))
        else:
            LOGGER.warning(
                "Not enough distinct %s values for time-based split; using random.",
                time_col,
            )

    if train_df is None or test_df is None:
        train_df, test_df = df.randomSplit([train_ratio, 1.0 - train_ratio], seed)
        split_method = "random"
    else:
        split_method = "time-based"

    LOGGER.info("Writing train set to %s", train_dir)
    train_df.write.mode("overwrite").parquet(path_as_str(train_dir))
    LOGGER.info("Writing test set to %s", test_dir)
    test_df.write.mode("overwrite").parquet(path_as_str(test_dir))
    LOGGER.info("Split method: %s", split_method)

    lr = LogisticRegression(
        labelCol=label_col,
        featuresCol=features_col,
        maxIter=int(training_cfg.get("max_iter", 50)),
        regParam=float(training_cfg.get("reg_param", 0.01)),
        elasticNetParam=float(training_cfg.get("elastic_net_param", 0.0)),
    )

    model = lr.fit(train_df)
    LOGGER.info("Saving model to %s", model_dir)
    model.write().overwrite().save(path_as_str(model_dir))

    spark.stop()


if __name__ == "__main__":
    main()
