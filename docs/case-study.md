# Case Study — Cloud Analytics ML Pipeline (PySpark)

## Executive Summary
This project is a reproducible clickstream analytics pipeline built on Spark MLlib. It ingests raw events, generates session‑level features, trains and evaluates a baseline conversion model, and exports dashboard‑ready aggregates. The key focus is **deterministic outputs across local and GCP Dataproc Serverless** using a configuration‑only portability model.

**Core deliverables**
- Schema‑enforced ingestion + parquet partitioning
- Sessionization + feature engineering with leakage filtering
- MLlib training + evaluation with time‑based split (fallback to random)
- Reports and dashboard artifacts decoupled from heavy compute
- Local ↔ cloud parity via `config.yaml` / `config.gcp.yaml`

---

## Problem
Analytics pipelines frequently break when moving from a developer laptop to the cloud. Paths change, configs drift, and feature sets become inconsistent. When outputs are not deterministic, dashboards cannot be trusted and model decisions are hard to audit.

**Pain points addressed**
- Inconsistent outputs across environments (local vs cloud)
- Slow iteration due to re‑running heavy jobs
- Data leakage in features that inflate metrics
- Unclear lineage between model runs and dashboards

## Why This Matters
- **Reproducibility**: same inputs → same outputs across environments
- **Speed**: faster iteration with cached parquet data and consistent configs
- **Auditability**: outputs are traceable to known configs and artifacts
- **Cost control**: dashboard reads precomputed aggregates (no live Spark)

## Real‑world framing (what this models)
- **Product analytics**: conversion funnels and propensity scoring
- **Sessionization**: user behavior must be grouped into sessions to model intent
- **Parquet partitioning**: keeps large event logs query‑friendly and repeatable
- **Leakage filtering**: prevents training on fields that “peek” at outcomes
- **Time‑based split**: simulates real deployment drift; random fallback if dates are missing
- **Dashboard aggregates**: precomputed metrics keep UI fast and cheap

---

## System Overview

### Architecture & Data Flow
See the full diagram in `docs/architecture.mmd`.

```
raw CSVs
  -> to_parquet (clean + partition)
    -> session_features (aggregate + label)
      -> train/test split + model
        -> evaluation metrics + plot
          -> dashboard aggregates (JSON + parquet)
            -> Streamlit app
```

### Core components
- **Orchestration**: Makefile targets call pipeline entry points.
- **Config**: `config.yaml` (local) and `config.gcp.yaml` (GCS).
- **Compute**: Spark session in `src/clickstream/spark.py`.
- **Storage**: local FS or GCS via `src/clickstream/utils/storage.py`.
- **Modeling**: MLlib logistic regression with leakage filtering.
- **Reporting**: JSON/CSV metrics + figures in `reports/`.
- **Dashboard**: Streamlit reads precomputed aggregates.

---

## Pipeline Stages (What Happens & Why)

### 1) Ingestion → `to_parquet`
- **Why**: parquet partitioning improves query speed and repeatability.
- **What**: schema‑enforced CSV parsing, clean/partitioned output.

### 2) Features → `build_features`
- **Why sessionization**: conversion and funnel signals are session‑level, not row‑level.
- **What**: aggregate events per session, create labels, compute features.

### 3) Train/Eval → `train.py`
- **Why leakage filtering**: prevent inflated metrics from target leakage.
- **Why time‑based split**: mimics real‑world deployment drift; fallback to random if no dates.
- **What**: MLlib logistic regression + evaluation metrics.

### 4) Exports → `export_dashboard_data.py`
- **Why**: dashboards should not run heavy Spark jobs.
- **What**: precompute aggregates into `reports/dashboard` for UI.

---

## Reproducibility Contract

**Deterministic / controlled**
- Input paths and processing settings are pinned in config files.
- Feature columns and leakage denylist are explicit in config.
- Outputs are written to fixed artifact locations (`reports/`, `models/`).

**Potentially non‑deterministic**
- Random split when time‑based split isn’t possible.
- Spark execution order can vary slightly by cluster.

**How to rerun**
```bash
make rerun_all_force
```
This wipes prior outputs and rebuilds artifacts from raw data.

---

## Local ↔ GCP Parity

**Goal**: identical pipeline, different storage + execution layer.

- `config.yaml` → local paths
- `config.gcp.yaml` → `gs://` paths and Dataproc settings
- **No code changes required** to switch environments

**GCP execution**
```bash
bash scripts/gcp_run_all.sh
```

---

## Dataset Strategy

Why externalize the dataset:
- Git repositories should not carry large or sensitive datasets.
- Public GCS data enables quick reproducibility for reviewers.
- Teams can bring their own dataset without changing code.

Options:
1) **Public GCS (default)** via `./scripts/download_data.sh`
2) **BYO dataset** in `data/raw/` (matching `dataset.file_pattern`)

---

## Outputs & Artifacts

**Reports**
- `reports/ingestion_summary.json` — counts + time range
- `reports/features_summary.json` — rows + positive rate
- `reports/metrics.json` — AUC and threshold metrics
- `reports/metrics.csv` — CSV version for quick review
- `reports/figures/metrics.png` — metric visualization

**Dashboard aggregates** (in `reports/dashboard/`)
- `funnel_daily.parquet`, `conversion_by_weekday.parquet`, etc.

**Interpretation of `metrics.json`**
- `roc_auc`: model ranking quality
- `threshold_*`: precision/recall metrics at chosen threshold
- `accuracy`: classification accuracy at threshold

---

## Engineering Tradeoffs

- **Spark vs pandas**: Spark scales and aligns with GCP execution; pandas would be simpler but not portable for large data.
- **Serverless vs long‑lived clusters**: Serverless reduces idle cost and simplifies ops, at the cost of longer cold starts.
- **Public dataset vs committed data**: externalization keeps repo light and avoids data governance issues.
- **Precomputed dashboard**: avoids recomputation, ensures consistent UI; trades off real‑time freshness.

---

## Limitations & Next Steps

**Current limitations**
- Baseline model only (logistic regression)
- No experiment tracking server or model registry
- Dashboard is static based on exported aggregates

**Next steps**
- Add MLflow tracking + model registry
- CI to validate pipeline outputs
- Scheduled Dataproc runs + monitoring

---

## How to Run (Local)

```bash
make setup
./scripts/download_data.sh
make rerun_all_force
make dashboard
```

---

## How to Run (GCP Dataproc Serverless)

```bash
make gcp_auth_check
make gcp_setup
make gcp_upload_raw
make gcp_package
make gcp_run_all
```
