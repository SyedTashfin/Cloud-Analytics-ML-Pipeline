#!/bin/sh
set -eu

echo "Checking gcloud CLI..."
if ! command -v gcloud >/dev/null 2>&1; then
  echo "ERROR: gcloud not found. Install the Google Cloud SDK first."
  exit 1
fi

PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
if [ -z "$PROJECT" ] || [ "$PROJECT" = "(unset)" ]; then
  echo "ERROR: gcloud project is not set."
  echo "Run: gcloud config set project clickstream-bigdata"
  exit 1
fi
echo "Project: $PROJECT"

REGION="$(gcloud config get-value dataproc/region 2>/dev/null || true)"
if [ -z "$REGION" ] || [ "$REGION" = "(unset)" ]; then
  REGION="$(gcloud config get-value compute/region 2>/dev/null || true)"
fi
if [ -z "$REGION" ] || [ "$REGION" = "(unset)" ]; then
  echo "ERROR: gcloud region is not set."
  echo "Run: gcloud config set dataproc/region europe-west1"
  exit 1
fi
echo "Region: $REGION"

BILLING_ENABLED="$(gcloud billing projects describe "$PROJECT" \
  --format="value(billingEnabled)" 2>/dev/null || true)"
if [ "$BILLING_ENABLED" != "True" ] && [ "$BILLING_ENABLED" != "true" ]; then
  echo "ERROR: Billing is not enabled for project $PROJECT."
  echo "Enable billing in the Cloud Console and try again."
  exit 1
fi
echo "Billing: enabled"
