from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import yaml


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for raw data verification."""
    parser = argparse.ArgumentParser(
        description="Verify raw CSV files exist and exceed a minimum size."
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml",
    )
    parser.add_argument(
        "--min-size-mb",
        type=float,
        default=None,
        help="Minimum file size in MB (default: 100 or config override)",
    )
    return parser.parse_args()


def load_config(config_path: Path) -> Dict:
    """Load a YAML config file into a dictionary."""
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Config at {config_path} is not a mapping")
    return data


def resolve_path(base_dir: Path, path_value: str) -> Path:
    """Resolve a path relative to the config file directory."""
    path = Path(path_value)
    return path if path.is_absolute() else (base_dir / path).resolve()


def get_data_raw_path(config: Dict, config_path: Path) -> Path:
    """Return the resolved data/raw path from config."""
    base_dir = config_path.resolve().parent
    raw_value = config.get("paths", {}).get("data_raw", "data/raw")
    return resolve_path(base_dir, raw_value)


def get_file_pattern(config: Dict) -> str:
    """Return the filename pattern from config."""
    dataset_cfg = config.get("dataset", {})
    return (
        dataset_cfg.get("file_pattern")
        or dataset_cfg.get("raw_filename_template")
        or "clickstream_{month}.csv"
    )


def get_months(config: Dict) -> List[str]:
    """Return configured months from config."""
    dataset_cfg = config.get("dataset", {})
    months = dataset_cfg.get("months", [])
    if not months:
        raise ValueError("No months configured under dataset.months")
    return months


def get_min_size_mb(config: Dict, cli_value: float | None) -> float:
    """Resolve the minimum file size threshold in MB."""
    if cli_value is not None:
        return cli_value
    dataset_cfg = config.get("dataset", {})
    return float(dataset_cfg.get("min_raw_size_mb", 100))


def check_file(path: Path, min_size_bytes: int) -> Tuple[bool, str]:
    """Check a file for existence and minimum size."""
    if not path.exists():
        return False, "missing"

    size_bytes = path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)
    if size_bytes < min_size_bytes:
        return False, f"size {size_mb:.2f} MB < {min_size_bytes / (1024 * 1024):.2f} MB"

    return True, f"size {size_mb:.2f} MB"


def iter_expected_files(
    raw_dir: Path, months: Iterable[str], file_pattern: str
) -> List[Path]:
    """Build a list of expected raw file paths."""
    return [raw_dir / file_pattern.format(month=month) for month in months]


def main() -> None:
    """Verify raw data files exist and are above the size threshold."""
    args = parse_args()
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)

    config = load_config(config_path)
    raw_dir = get_data_raw_path(config, config_path)
    months = get_months(config)
    file_pattern = get_file_pattern(config)
    min_size_mb = get_min_size_mb(config, args.min_size_mb)
    min_size_bytes = int(min_size_mb * 1024 * 1024)

    expected_files = iter_expected_files(raw_dir, months, file_pattern)

    any_fail = False
    for path in expected_files:
        ok, detail = check_file(path, min_size_bytes)
        status = "PASS" if ok else "FAIL"
        print(f"{status}: {path} ({detail})")
        if not ok:
            any_fail = True

    if any_fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
