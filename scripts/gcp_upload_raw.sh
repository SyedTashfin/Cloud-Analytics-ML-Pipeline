#!/bin/sh
set -eu

if ! command -v gsutil >/dev/null 2>&1; then
  echo "ERROR: gsutil not found. Install the Google Cloud SDK first."
  exit 1
fi

RAW_BUCKET="gs://clickstream-bigdata-raw"
LOCAL_DIR="data/raw"

if ! ls "$LOCAL_DIR"/*.csv >/dev/null 2>&1; then
  echo "ERROR: No CSV files found in $LOCAL_DIR"
  exit 1
fi

echo "Uploading raw CSVs to $RAW_BUCKET..."
gsutil -m cp "$LOCAL_DIR"/*.csv "$RAW_BUCKET"/

CSV_COUNT="$(gsutil ls "$RAW_BUCKET"/*.csv 2>/dev/null | wc -l | tr -d ' ')"
if [ "${CSV_COUNT:-0}" -lt 1 ]; then
  echo "ERROR: No CSV files found in $RAW_BUCKET after upload."
  exit 1
fi

TOTAL_BYTES="$(gsutil du -s "$RAW_BUCKET" | awk '{print $1}')"
echo "Upload complete. CSVs in bucket: $CSV_COUNT"
echo "Total raw bucket size: ${TOTAL_BYTES:-0} bytes"
