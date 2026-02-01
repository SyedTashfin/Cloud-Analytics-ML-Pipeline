from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

from pyspark.sql import SparkSession

from utils.storage import (
    is_gcs_path,
    list_dir,
    path_as_str,
    path_exists,
    path_status,
)

PathLike = Union[str, Path]


def assert_path_exists_and_nonempty(
    path: PathLike, hint: str, spark: Optional[SparkSession] = None
) -> None:
    """Ensure a path exists and is non-empty."""
    if is_gcs_path(path):
        if spark is None:
            raise FileNotFoundError(hint)
        if not path_exists(path, spark):
            raise FileNotFoundError(hint)
        status = path_status(path, spark)
        if status is None:
            raise FileNotFoundError(hint)
        if status.isFile():
            if status.getLen() == 0:
                raise ValueError(hint)
            return
        if status.isDirectory():
            if len(list_dir(path, spark)) == 0:
                raise ValueError(hint)
            return
        raise FileNotFoundError(hint)

    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(hint)

    if path_obj.is_file():
        if path_obj.stat().st_size == 0:
            raise ValueError(hint)
        return

    if not path_obj.is_dir():
        raise FileNotFoundError(hint)

    has_contents = any(child for child in path_obj.iterdir())
    if not has_contents:
        raise ValueError(hint)


def assert_parquet_readable(spark: SparkSession, path: PathLike, hint: str) -> None:
    """Ensure parquet data can be read and contains at least one row."""
    assert_path_exists_and_nonempty(path, hint, spark=spark)
    try:
        df = spark.read.parquet(path_as_str(path))
        if df.limit(1).count() == 0:
            raise ValueError(hint)
    except Exception as exc:
        raise RuntimeError(hint) from exc
