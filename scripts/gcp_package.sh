#!/bin/sh
set -eu

if ! command -v zip >/dev/null 2>&1; then
  echo "ERROR: zip not found. Install zip first."
  exit 1
fi
if ! command -v gsutil >/dev/null 2>&1; then
  echo "ERROR: gsutil not found. Install the Google Cloud SDK first."
  exit 1
fi

ZIP_NAME="clickstream_pkg.zip"
DEST_BUCKET="gs://clickstream-bigdata-reports"

if [ ! -d "src/clickstream" ]; then
  echo "ERROR: src/clickstream not found. Run from repo root."
  exit 1
fi

rm -f "$ZIP_NAME"
echo "Packaging source bundle..."
(cd src && zip -r "../$ZIP_NAME" clickstream >/dev/null)

echo "Uploading package to $DEST_BUCKET/$ZIP_NAME..."
gsutil cp "$ZIP_NAME" "$DEST_BUCKET"/

if ! gsutil ls "$DEST_BUCKET/$ZIP_NAME" >/dev/null 2>&1; then
  echo "ERROR: Upload failed for $DEST_BUCKET/$ZIP_NAME"
  exit 1
fi

echo "Package uploaded: $DEST_BUCKET/$ZIP_NAME"
