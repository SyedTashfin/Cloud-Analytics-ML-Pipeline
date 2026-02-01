# Cloud Analytics ML Pipeline — One‑Pager

**One‑liner**: Reproducible Spark ML pipeline (ingest → features → train → evaluate → dashboard artifacts) with local ↔ GCP Dataproc Serverless parity.

## Problem
Analytics ML pipelines often drift across environments: local runs don’t match cloud outputs, dashboards can’t be trusted, and teams waste time re‑running feature jobs. This project fixes that by making storage paths, configs, and outputs deterministic.

## What I built
- Spark ingestion with schema enforcement + parquet partitioning.
- Session-level feature engineering + leakage filtering.
- Baseline MLlib logistic regression with time‑based split when possible.
- Dashboard artifacts exported to `reports/` for fast Streamlit UI.
- Config‑driven portability (`config.yaml` vs `config.gcp.yaml`).

## Why it matters
- **Reproducibility** across laptop and cloud.
- **Cost control** by decoupling dashboards from heavy compute.
- **Auditability** with consistent metrics and artifacts per run.

## Run locally (copy/paste)
```bash
make setup
./scripts/download_data.sh
make rerun_all_force
make dashboard
```

## Cloud parity (GCP Dataproc Serverless)
```bash
bash scripts/gcp_run_all.sh
```

## Key artifacts
- `reports/metrics.json`, `reports/metrics.csv`
- `reports/figures/metrics.png`
- `reports/dashboard/*.parquet`
