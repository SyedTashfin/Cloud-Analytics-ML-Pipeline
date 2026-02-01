#!/bin/sh
set -eu

if ! command -v gcloud >/dev/null 2>&1; then
  echo "ERROR: gcloud not found. Install the Google Cloud SDK first."
  exit 1
fi
if ! command -v gsutil >/dev/null 2>&1; then
  echo "ERROR: gsutil not found. Install the Google Cloud SDK first."
  exit 1
fi

echo "Enabling required APIs..."
gcloud services enable storage.googleapis.com dataproc.googleapis.com

check_bucket() {
  BUCKET="$1"
  if ! gsutil ls -b "$BUCKET" >/dev/null 2>&1; then
    echo "ERROR: Bucket not found: $BUCKET"
    exit 1
  fi
  echo "Bucket OK: $BUCKET"
}

check_bucket "gs://clickstream-bigdata-raw"
check_bucket "gs://clickstream-bigdata-processed"
check_bucket "gs://clickstream-bigdata-reports"
