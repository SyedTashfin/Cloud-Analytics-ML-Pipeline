from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

PathLike = Union[str, Path]


def is_gcs_path(path: PathLike) -> bool:
    """Return True if a path points to Google Cloud Storage."""
    value = str(path)
    return value.startswith("gs://") or value.startswith("gs:/")


def normalize_path(path: PathLike) -> str:
    """Normalize a path string for Spark/Hadoop usage."""
    value = str(path)
    if value.startswith("gs:/") and not value.startswith("gs://"):
        return value.replace("gs:/", "gs://", 1)
    return value


def path_as_str(path: PathLike) -> str:
    """Return a safe string path for Spark reads/writes."""
    return normalize_path(path)


def resolve_child_path(base: Path, value: str) -> Path:
    """Resolve a child path, honoring absolute and GCS paths."""
    if is_gcs_path(value):
        return Path(value)
    child = Path(value)
    if child.is_absolute():
        return child
    return base / value


def _get_fs_and_path(
    spark: "SparkSession", path: PathLike
) -> Tuple[object, object, str]:
    uri = normalize_path(path)
    jvm = spark._jvm
    jpath = jvm.org.apache.hadoop.fs.Path(uri)
    fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs, jpath, uri


def path_exists(path: PathLike, spark: Optional["SparkSession"] = None) -> bool:
    """Check if a path exists locally or in GCS."""
    if is_gcs_path(path):
        if spark is None:
            raise ValueError("Spark session required to check GCS paths.")
        fs, jpath, _ = _get_fs_and_path(spark, path)
        return bool(fs.exists(jpath))
    return Path(path).exists()


def path_status(path: PathLike, spark: "SparkSession") -> Optional[object]:
    """Return Hadoop FileStatus for a GCS path, or None if missing."""
    fs, jpath, _ = _get_fs_and_path(spark, path)
    if not fs.exists(jpath):
        return None
    return fs.getFileStatus(jpath)


def list_dir(path: PathLike, spark: "SparkSession") -> list:
    """List contents of a GCS directory."""
    fs, jpath, _ = _get_fs_and_path(spark, path)
    return list(fs.listStatus(jpath))


def delete_path(path: PathLike, spark: "SparkSession") -> None:
    """Delete a GCS path recursively."""
    fs, jpath, _ = _get_fs_and_path(spark, path)
    fs.delete(jpath, True)


def read_text(
    path: PathLike, spark: Optional["SparkSession"] = None, encoding: str = "utf-8"
) -> str:
    """Read a text file from local disk or GCS."""
    if is_gcs_path(path):
        if spark is None:
            raise ValueError("Spark session required to read GCS paths.")
        fs, jpath, _ = _get_fs_and_path(spark, path)
        stream = fs.open(jpath)
        jvm = spark._jvm
        reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream))
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        stream.close()
        return "\n".join(lines)
    return Path(path).read_text(encoding=encoding)


def write_text(
    path: PathLike,
    text: str,
    spark: Optional["SparkSession"] = None,
    encoding: str = "utf-8",
) -> None:
    """Write a text file to local disk or GCS."""
    if is_gcs_path(path):
        if spark is None:
            raise ValueError("Spark session required to write GCS paths.")
        fs, jpath, _ = _get_fs_and_path(spark, path)
        output = fs.create(jpath, True)
        output.write(bytearray(text, encoding))
        output.close()
        return
    Path(path).write_text(text, encoding=encoding)


def write_bytes(
    path: PathLike, data: bytes, spark: Optional["SparkSession"] = None
) -> None:
    """Write binary data to local disk or GCS."""
    if is_gcs_path(path):
        if spark is None:
            raise ValueError("Spark session required to write GCS paths.")
        fs, jpath, _ = _get_fs_and_path(spark, path)
        output = fs.create(jpath, True)
        output.write(bytearray(data))
        output.close()
        return
    Path(path).write_bytes(data)
