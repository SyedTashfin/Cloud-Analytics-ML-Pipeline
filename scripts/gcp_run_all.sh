#!/bin/sh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"

run_batch() {
  NAME="$1"
  FILE="$2"
  "$SCRIPT_DIR/gcp_submit.sh" "${NAME}-${TIMESTAMP}" "$FILE"
}

cd "$ROOT_DIR"

run_batch "clickstream-to-parquet" "src/clickstream/pipelines/to_parquet.py"
run_batch "clickstream-build-features" "src/clickstream/pipelines/build_features.py"
run_batch "clickstream-train" "src/clickstream/pipelines/train.py"
run_batch "clickstream-evaluate" "src/clickstream/pipelines/evaluate.py"
run_batch "clickstream-plots" "src/clickstream/pipelines/plots.py"
