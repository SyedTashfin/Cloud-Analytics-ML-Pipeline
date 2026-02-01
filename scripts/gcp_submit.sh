#!/bin/sh
set -eu

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <batch_name> <py_file>"
  exit 1
fi

if ! command -v gcloud >/dev/null 2>&1; then
  echo "ERROR: gcloud not found. Install the Google Cloud SDK first."
  exit 1
fi

BATCH_NAME="$1"
PY_FILE="$2"
DEPS_BUCKET="gs://clickstream-bigdata-reports"
ZIP_URI="$DEPS_BUCKET/clickstream_pkg.zip"
PIP_PACKAGES_FILE="requirements.gcp.txt"
SPARK_RESOURCE_PROPERTIES_DEFAULT="spark.executor.instances=2:spark.executor.cores=4:spark.executor.memory=8g:spark.driver.cores=4:spark.driver.memory=8g"
SPARK_RESOURCE_PROPERTIES="${SPARK_RESOURCE_PROPERTIES:-$SPARK_RESOURCE_PROPERTIES_DEFAULT}"

PIP_PACKAGES=""
if [ -f "$PIP_PACKAGES_FILE" ]; then
  PIP_PACKAGES="$(awk '
    NF && $1 !~ /^#/ {
      sub(/#.*/, "");
      gsub(/[[:space:]]+$/, "");
      if (length($0)) {
        printf "%s%s", (seen ? "," : ""), $0;
        seen = 1;
      }
    }
  ' "$PIP_PACKAGES_FILE")"
fi

if [ "${PY_FILE#gs://}" = "$PY_FILE" ] && [ ! -f "$PY_FILE" ]; then
  echo "ERROR: Python file not found: $PY_FILE"
  exit 1
fi
if [ ! -f "config.gcp.yaml" ]; then
  echo "ERROR: config.gcp.yaml not found. Run from repo root."
  exit 1
fi

REGION="$(gcloud config get-value dataproc/region 2>/dev/null || true)"
if [ -z "$REGION" ] || [ "$REGION" = "(unset)" ]; then
  REGION="europe-west1"
fi

PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
if [ -z "$PROJECT" ] || [ "$PROJECT" = "(unset)" ]; then
  PROJECT="clickstream-bigdata"
fi

echo "Submitting batch: $BATCH_NAME"
PROPERTIES_LIST=""
if [ -n "$PIP_PACKAGES" ]; then
  PROPERTIES_LIST="spark.dataproc.pip.packages=$PIP_PACKAGES"
fi
if [ -n "$SPARK_RESOURCE_PROPERTIES" ]; then
  if [ -n "$PROPERTIES_LIST" ]; then
    PROPERTIES_LIST="$PROPERTIES_LIST:$SPARK_RESOURCE_PROPERTIES"
  else
    PROPERTIES_LIST="$SPARK_RESOURCE_PROPERTIES"
  fi
fi

if [ -n "$PROPERTIES_LIST" ]; then
  gcloud dataproc batches submit pyspark "$PY_FILE" \
    --batch="$BATCH_NAME" \
    --region="$REGION" \
    --deps-bucket="$DEPS_BUCKET" \
    --py-files="$ZIP_URI" \
    --files="config.gcp.yaml" \
    --properties="^:^$PROPERTIES_LIST" \
    -- --config config.gcp.yaml
else
  gcloud dataproc batches submit pyspark "$PY_FILE" \
    --batch="$BATCH_NAME" \
    --region="$REGION" \
    --deps-bucket="$DEPS_BUCKET" \
    --py-files="$ZIP_URI" \
    --files="config.gcp.yaml" \
    -- --config config.gcp.yaml
fi

echo "Batch submitted: $BATCH_NAME"
echo "Console URL: https://console.cloud.google.com/dataproc/batches/$BATCH_NAME?project=$PROJECT&region=$REGION"
