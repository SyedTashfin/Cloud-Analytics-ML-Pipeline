#!/usr/bin/env bash
set -euo pipefail

: "${GCS_DATASET_URI:=gs://clickstream-bigdata-raw}"
: "${RAW_DIR:=data/raw}"

mkdir -p "${RAW_DIR}"

echo "Downloading from: ${GCS_DATASET_URI}"
echo "Destination: ${RAW_DIR}"

gsutil -m cp -r "${GCS_DATASET_URI}/*" "${RAW_DIR}/"

echo "Done. Dataset is in ${RAW_DIR} (ignored by git)."
