# 5‑Minute Demo Script (Recruiter Quick Proof)

## 0) What this demonstrates (skills map)

| Repo part | What it proves | Skills |
| --- | --- | --- |
| `src/clickstream/pipelines/to_parquet.py` | schema enforcement + partitioning | Spark ingestion, data quality |
| `src/clickstream/pipelines/build_features.py` | sessionization + labels | feature engineering |
| `src/clickstream/pipelines/train.py` | MLlib pipeline + leakage filters | ML evaluation, model hygiene |
| `config.yaml` / `config.gcp.yaml` | environment parity | reproducibility, cloud portability |
| `reports/` + `dashboard/` | artifacts + dashboard | analytics storytelling, UX-ready outputs |

## 1) Setup (30–60s)

```bash
make setup
./scripts/download_data.sh
```

If you want a smoke run:

```bash
make smoke
```

## 2) Run the full pipeline (2 min)

```bash
make rerun_all_force
```

Talk track:
- Ingests raw clickstream CSVs into parquet with an explicit schema.
- Builds session-level features + labels.
- Trains a baseline logistic regression and evaluates with time-based split when available.
- Exports dashboard aggregates so the UI is decoupled from heavy Spark jobs.

## 3) Show the artifacts (1 min)

```bash
ls reports
cat reports/metrics.json
```

Callouts:
- `metrics.json` contains AUC and threshold metrics for reproducibility.
- `reports/figures/metrics.png` shows ROC/threshold curves.
- `reports/dashboard/*.parquet` are the UI-ready aggregates.

## 4) Launch the dashboard (1 min)

```bash
make dashboard
```

Talk track:
- Streamlit reads precomputed aggregates only (fast, cheap, consistent).
- This makes the dashboard deterministic for demo and avoids re-running Spark.

## 5) Cloud parity (30–60s)

```bash
bash scripts/gcp_run_all.sh
```

Talk track:
- Same steps, same config model, just `config.gcp.yaml` + Dataproc Serverless.
- No code changes required to move from laptop to GCP.

## Optional: What to open during the demo

- `reports/metrics.json`
- `reports/figures/metrics.png`
- `dashboard/app.py`
- `config.yaml` and `config.gcp.yaml`
