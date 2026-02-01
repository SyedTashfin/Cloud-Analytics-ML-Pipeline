#!/usr/bin/env bash
set -euo pipefail

: "${GCS_DATASET_URI:=gs://clickstream-bigdata-raw}"

mkdir -p data
echo "Downloading from: ${GCS_DATASET_URI}"
gsutil -m cp -r "${GCS_DATASET_URI}/*" ./data/
echo "Done. Dataset is in ./data (ignored by git)."
