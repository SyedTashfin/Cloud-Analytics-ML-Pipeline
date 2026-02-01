from __future__ import annotations

import argparse
import json
import logging
import io

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from clickstream.config import ensure_dirs, load_config, resolve_paths
from clickstream.spark import get_spark_session
from clickstream.utils.stage_guard import assert_path_exists_and_nonempty
from clickstream.utils.storage import is_gcs_path, read_text, write_bytes

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Generate report plots.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    return parser.parse_args()


def main() -> None:
    """Create metric plots from evaluation outputs."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    config = load_config(args.config)
    paths = resolve_paths(config, args.config)

    reports_dir = paths["reports"]
    figures_dir = paths["figures"]
    ensure_dirs(reports_dir, figures_dir)

    metrics_path = reports_dir / "metrics.json"
    spark = get_spark_session(config) if is_gcs_path(metrics_path) else None
    assert_path_exists_and_nonempty(
        metrics_path,
        "Missing reports/metrics.json. Run `make evaluate`.",
        spark=spark,
    )

    if is_gcs_path(metrics_path):
        metrics = json.loads(read_text(metrics_path, spark=spark))
    else:
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    keys = ["auc", "accuracy", "precision", "recall", "positive_rate"]
    values = [metrics.get(key, 0.0) for key in keys]

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(keys, values, color="#2F6B7C")
    ax.set_ylim(0, 1)
    ax.set_ylabel("Score")
    ax.set_title("Model Metrics")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    output_path = figures_dir / "metrics.png"
    LOGGER.info("Saving plot to %s", output_path)
    fig.tight_layout()
    if is_gcs_path(output_path):
        buffer = io.BytesIO()
        fig.savefig(buffer, format="png", dpi=150)
        write_bytes(output_path, buffer.getvalue(), spark=spark)
    else:
        fig.savefig(output_path, dpi=150)
    plt.close(fig)
    if spark is not None:
        spark.stop()


if __name__ == "__main__":
    main()
