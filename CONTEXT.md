# Project Context: E-Commerce Clickstream (PySpark)

## Purpose
This repository is a reproducible clickstream analytics pipeline built on PySpark.
It ingests raw e-commerce event logs, generates session-level features, trains a
logistic regression model to predict session purchases, evaluates performance,
and produces aggregates for a Streamlit dashboard.

## Repository Layout (Key Paths)
- `README.md`: High-level usage, local + GCP instructions.
- `Makefile`: Convenience targets that wire the pipeline together.
- `config.yaml`: Local paths + dataset and model settings.
- `config.gcp.yaml`: GCS paths + Dataproc settings.
- `src/clickstream/`: Main Python package (Spark pipelines + utilities).
- `src/utils/`: Utility scripts duplicated for non-package entrypoints.
- `src/01_download_data.py`: Kaggle dataset downloader (requires Kaggle API).
- `dashboard/`: Streamlit app + Dockerfile + requirements.
- `data/`: Raw and processed datasets (CSV + parquet).
- `models/`: Trained Spark ML artifacts (default: `models/logreg`).
- `reports/`: Metrics JSON/CSV, figures, dashboard aggregates, report templates.
- `scripts/`: GCP helper scripts for Dataproc Serverless execution.

## Configuration Summary
### `config.yaml` (local)
- `paths`: local storage roots for raw/interim/processed data, reports, models.
- `dataset`: months, file naming, synthetic generation settings, event types.
- `features`: numeric feature list, label column, feature vector column.
- `leakage_columns`: denylist of columns removed from modeling.
- `split`: time-based split by `session_date` or random fallback.
- `training`: logistic regression hyperparameters.
- `evaluation`: classification threshold (default 0.5).
- `processing`: overwrite behavior, row limits, local chunk size.
- `spark`: app name, master, shuffle partitions, timezone.

### `config.gcp.yaml` (GCS + Dataproc)
- Mirrors `config.yaml` but all paths point to GCS buckets.
- Adds processing controls for output coalescing and file limits.

## Data Expectations
### Raw CSV Schema (expected by `to_parquet`)
The ingestion step assumes Kaggle-style clickstream CSVs with these columns:
- `event_time`, `event_type`, `product_id`, `category_id`, `category_code`,
  `brand`, `price`, `user_id`, `user_session`

### Synthetic Data Generator
`clickstream.pipelines.download_data` can generate synthetic CSVs when
`dataset.url_template` is empty. Its output columns are:
- `event_time`, `user_id`, `session_id`, `event_type`, `product_id`, `price`,
  `currency`, `device_type`, `browser`, `referrer`, `country`,
  `session_duration`, `pages_viewed`, `month`

If you use synthetic data, ensure the ingestion schema matches or update
`to_parquet.py` accordingly.

### Processed Data Outputs
In `data/processed/` (or GCS equivalents):
- `events_parquet/`: partitioned by `event_date`
- `session_features/`: session-level aggregations
- `train/`, `test/`: train/test splits for modeling

## Pipeline Overview (Spark)
### 1) Download or Generate Raw Data
File: `src/clickstream/pipelines/download_data.py`
- Downloads CSVs from `dataset.url_template` or generates synthetic data.
- Writes to `paths.data_raw`.

### 2) CSV → Parquet Ingestion
File: `src/clickstream/pipelines/to_parquet.py`
- Reads raw CSVs with a strict schema.
- Normalizes timestamps (handles trailing `UTC`).
- Filters invalid rows and negative prices.
- Writes partitioned parquet by `event_date` with chunking for local runs.
- Writes `reports/ingestion_summary.json`.

### 3) Feature Engineering
File: `src/clickstream/pipelines/build_features.py`
- Aggregates by `user_session`.
- Builds session-level features:
  - `total_events`, `num_views`, `num_carts`, `num_purchases`
  - `unique_products`, `unique_categories`
  - `session_duration_seconds`, `avg_price_viewed`, `max_price_viewed`
  - `hour_of_day`, `day_of_week`, `is_weekend`
  - `purchase_in_session` (label)
- Writes `data/processed/session_features` and `reports/features_summary.json`.

### 4) Train Model
File: `src/clickstream/pipelines/train.py`
- Filters leakage-prone features via `leakage_columns` and `purchase` heuristics.
- Uses time-based split on `session_date` when available.
- Vectorizes numeric features with `VectorAssembler`.
- Trains Spark `LogisticRegression`.
- Writes train/test sets and model artifacts to `models/logreg/`.

### 5) Evaluate Model
File: `src/clickstream/pipelines/evaluate.py`
- Loads test/train and model.
- Computes AUC + confusion-matrix-based metrics.
- Writes `reports/metrics.json` and `reports/metrics.csv`.

### 6) Generate Plots
File: `src/clickstream/pipelines/plots.py`
- Reads `reports/metrics.json`.
- Writes `reports/figures/metrics.png`.

### 7) Export Dashboard Aggregates
File: `src/clickstream/pipelines/export_dashboard_data.py`
Creates lightweight aggregates for Streamlit:
- KPIs, model metrics, propensity KPIs, and drivers (`.json`)
- Parquet aggregates for:
  - Funnel events by day
  - Purchases by hour
  - Conversion by weekday
  - Top categories/brands
  - Session distributions
  - Propensity daily, histogram, threshold curves
  - Calibration bins

## Dashboard (Streamlit)
File: `dashboard/app.py`
- Reads data from `reports/dashboard/` (or `DASHBOARD_DATA_DIR` env).
- Supports GCS paths via `gcsfs`.
- Provides conversion analytics and propensity insights.
- Uses IBM Plex fonts and Plotly for visuals.

Environment variables:
- `DASHBOARD_DATA_DIR`: dashboard aggregates root (local or `gs://`)
- `REPORTS_ROOT`: reports root for fallback metrics

## Utilities
### Spark + GCS Helpers
File: `src/clickstream/utils/storage.py`
- Detects GCS paths and reads/writes via Spark’s Hadoop FS.

### Stage Guards
File: `src/clickstream/utils/stage_guard.py`
- Validates expected inputs (parquet availability / non-empty).

### Environment Checks
File: `src/clickstream/utils/env_check.py`
- Verifies Python, Java, Spark, and filesystem access.

### Raw File Verification
File: `src/clickstream/utils/verify_raw.py`
- Confirms raw CSVs exist and exceed size threshold.

Note: `src/utils/` duplicates these helpers for non-package scripts.

## GCP / Dataproc Serverless
Scripts in `scripts/` automate:
- `gcp_auth_check.sh`: validates gcloud config + billing.
- `gcp_setup.sh`: enables APIs and checks buckets.
- `gcp_upload_raw.sh`: uploads raw CSVs to GCS.
- `gcp_package.sh`: zips `src/clickstream` and uploads bundle.
- `gcp_submit.sh`: submits a Dataproc Serverless batch job.
- `gcp_run_all.sh`: runs the full pipeline on GCP.

## How to Run (Local)
Use the Makefile targets:
- `make setup` (venv + deps)
- `make download_data`
- `make to_parquet`
- `make build_features`
- `make train`
- `make evaluate`
- `make plots`
- `make dashboard_data`
- `make dashboard`

## Dependencies
Key Python deps (local):
- `pyspark`, `pandas`, `pyarrow`, `numpy`, `scikit-learn`
- `matplotlib`, `pyyaml`, `tqdm`, `streamlit`, `plotly`

Dashboard deps (Docker/streamlit):
- `streamlit`, `pandas`, `plotly`, `pyarrow`, `gcsfs`

Java 8 or 11 is required to run Spark locally.
