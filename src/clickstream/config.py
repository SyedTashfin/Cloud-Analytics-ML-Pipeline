from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml

from .utils.storage import is_gcs_path

def load_config(config_path: str | Path) -> Dict[str, Any]:
    """Load the project YAML configuration."""
    path = Path(config_path)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Config at {path} is not a mapping")
    return data


def resolve_paths(config: Dict[str, Any], config_path: str | Path) -> Dict[str, Path]:
    """Resolve paths relative to the config file location."""
    base_dir = Path(config_path).resolve().parent
    raw_paths = config.get("paths", {})
    resolved: Dict[str, Path] = {}
    for key, value in raw_paths.items():
        if value is None:
            continue
        if is_gcs_path(value):
            resolved[key] = Path(value)
            continue
        path = Path(value)
        if not path.is_absolute():
            path = (base_dir / path).resolve()
        resolved[key] = path
    return resolved


def ensure_dirs(*paths: Path) -> None:
    """Create directories if they do not exist."""
    for path in paths:
        if is_gcs_path(path):
            continue
        path.mkdir(parents=True, exist_ok=True)


def filter_feature_columns(
    features: Iterable[str],
    label_col: str,
    leakage_columns: Iterable[str] | None = None,
) -> Tuple[List[str], List[str]]:
    """Filter feature columns using a leakage denylist and purchase heuristics."""
    leakage_set = {col for col in (leakage_columns or []) if col}
    filtered: List[str] = []
    removed: List[str] = []
    for feature in features:
        feature_lower = feature.lower()
        if feature == label_col:
            removed.append(feature)
            continue
        if feature in leakage_set:
            removed.append(feature)
            continue
        if "purchase" in feature_lower and feature != label_col:
            removed.append(feature)
            continue
        filtered.append(feature)
    return filtered, removed
