# Cloud Analytics ML Pipeline — One‑pager

**En bref** : pipeline ML Spark reproductible (ingest → features → train → eval → exports dashboard) avec parité local ↔ Dataproc Serverless.

## Problème
Les pipelines analytics dérivent entre local et cloud : sorties incohérentes, dashboards peu fiables, et perte de temps à relancer les jobs. Ce projet rend les configs, chemins et outputs déterministes.

## Ce que j’ai construit
- Ingestion Spark avec schéma explicite + partitionnement parquet.
- Features de session + filtration de fuite (leakage).
- Modèle baseline MLlib (logistic regression) avec split temporel si possible.
- Exports dashboard dans `reports/` pour un UI Streamlit rapide.
- Portabilité par config (`config.yaml` vs `config.gcp.yaml`).

## Pourquoi c’est important
- **Reproductibilité** entre laptop et cloud.
- **Maîtrise des coûts** en découplant dashboard et calcul lourd.
- **Auditabilité** via des métriques et artifacts stables.

## Lancer en local (copy/paste)
```bash
make setup
./scripts/download_data.sh
make rerun_all_force
make dashboard
```

## Parité cloud (GCP Dataproc Serverless)
```bash
bash scripts/gcp_run_all.sh
```

## Artifacts clés
- `reports/metrics.json`, `reports/metrics.csv`
- `reports/figures/metrics.png`
- `reports/dashboard/*.parquet`
