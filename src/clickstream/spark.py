from __future__ import annotations

from typing import Any, Dict

from pyspark.sql import SparkSession


def get_spark_session(config: Dict[str, Any]) -> SparkSession:
    """Create a SparkSession from config settings."""
    spark_cfg = config.get("spark", {})
    app_name = spark_cfg.get("app_name", "clickstream")
    master = spark_cfg.get("master")
    shuffle_partitions = spark_cfg.get("shuffle_partitions", 64)
    timezone = spark_cfg.get("timezone", "UTC")

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    builder = builder.config("spark.driver.memory", "3g")
    builder = builder.config("spark.executor.memory", "3g")
    builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    builder = builder.config("spark.sql.files.maxPartitionBytes", "134217728")
    builder = builder.config("spark.default.parallelism", "64")
    builder = builder.config("spark.sql.adaptive.enabled", "false")
    builder = builder.config("spark.sql.session.timeZone", timezone)
    builder = builder.config("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    return builder.getOrCreate()
