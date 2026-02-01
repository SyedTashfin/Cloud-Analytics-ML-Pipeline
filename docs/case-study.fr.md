# Étude de cas — Cloud Analytics ML Pipeline (PySpark)

## Résumé exécutif
Ce projet est un pipeline d’analytics clickstream reproductible basé sur Spark MLlib. Il ingère des événements bruts, génère des features de session, entraîne/évalue un modèle de conversion baseline et exporte des agrégats prêts pour un dashboard. Le point clé : **des sorties déterministes en local et sur GCP Dataproc Serverless via la configuration uniquement**.

**Livrables clés**
- Ingestion avec schéma explicite + partitionnement parquet
- Sessionization + features avec filtrage de leakage
- Entraînement/évaluation MLlib avec split temporel (fallback aléatoire)
- Exports d’agrégats dashboard décorrélés du calcul lourd
- Parité local ↔ cloud via `config.yaml` / `config.gcp.yaml`

---

## Problème
Les pipelines analytics se cassent souvent entre laptop et cloud : chemins, configs et features divergent. Les dashboards deviennent peu fiables et l’auditabilité des modèles est faible.

**Douleurs adressées**
- Sorties incohérentes entre environnements
- Itérations lentes faute d’artifacts stables
- Leakage de features qui gonfle les métriques
- Manque de traçabilité entre runs et dashboard

## Pourquoi c’est important
- **Reproductibilité** : mêmes entrées → mêmes sorties
- **Vitesse** : itération rapide via parquet + configs stables
- **Auditabilité** : artifacts et métriques traçables
- **Coût** : dashboard basé sur agrégats pré‑calculés

## Contexte réel (ce que cela modélise)
- **Product analytics** : funnels de conversion et propensity scoring
- **Sessionization** : regrouper les événements par session pour capter l’intention
- **Parquet partitionné** : requêtes rapides et exécution reproductible
- **Filtrage leakage** : éviter d’entraîner sur des signaux trop proches de la cible
- **Split temporel** : refléter la dérive réelle ; fallback aléatoire si dates manquantes
- **Agrégats dashboard** : UI rapide, coûts maîtrisés

---

## Vue d’ensemble

### Architecture & flux de données
Voir `docs/architecture.mmd`.

```
CSVs bruts
  -> to_parquet (clean + partition)
    -> session_features (aggregate + label)
      -> train/test split + modèle
        -> métriques + plots
          -> agrégats dashboard (JSON + parquet)
            -> Streamlit
```

### Composants principaux
- **Orchestration** : Makefile
- **Config** : `config.yaml` (local), `config.gcp.yaml` (GCS)
- **Compute** : Spark (`src/clickstream/spark.py`)
- **Storage** : local ou GCS (`utils/storage.py`)
- **Modélisation** : MLlib logistic regression + leakage filtering
- **Reporting** : JSON/CSV + figures dans `reports/`
- **Dashboard** : Streamlit lit des agrégats pré‑calculés

---

## Étapes du pipeline

### 1) Ingestion → `to_parquet`
- **Pourquoi** : parquet améliore la performance et la reproductibilité.
- **Quoi** : schéma explicite + outputs partitionnés.

### 2) Features → `build_features`
- **Pourquoi sessioniser** : conversion et funnel sont des signaux de session.
- **Quoi** : agrégations par session + labels.

### 3) Train/Eval → `train.py`
- **Pourquoi filtrer le leakage** : éviter des métriques artificiellement élevées.
- **Pourquoi split temporel** : refléter le réel ; fallback aléatoire sinon.

### 4) Exports → `export_dashboard_data.py`
- **Pourquoi** : le dashboard ne doit pas lancer Spark.
- **Quoi** : agrégats exportés dans `reports/dashboard`.

---

## Contrat de reproductibilité

**Déterministe**
- Chemins et paramètres via config
- Colonnes de features + leakage denylist explicites
- Artifacts écrits à des emplacements stables

**Non‑déterministe**
- Split aléatoire si pas de dates
- Variations mineures d’exécution Spark

**Relancer**
```bash
make rerun_all_force
```

---

## Parité local ↔ GCP

**Objectif** : même pipeline, stockage et exécution différents.

- `config.yaml` → chemins locaux
- `config.gcp.yaml` → chemins `gs://` + paramètres Dataproc
- **Aucune modification de code** pour changer d’environnement

---

## Stratégie dataset

Pourquoi externaliser les données :
- Repo léger et conforme aux bonnes pratiques
- Dataset public pour reproductibilité rapide
- Support BYO dataset sans modifier le code

Options :
1) **GCS public** via `./scripts/download_data.sh`
2) **BYO dataset** dans `data/raw/`

---

## Artifacts & outputs

**Reports**
- `reports/ingestion_summary.json`
- `reports/features_summary.json`
- `reports/metrics.json`
- `reports/metrics.csv`
- `reports/figures/metrics.png`

**Aggregates dashboard**
- `reports/dashboard/*.parquet`

**Interpréter `metrics.json`**
- `roc_auc` : qualité de ranking
- `threshold_*` : précision/rappel au seuil
- `accuracy` : accuracy au seuil choisi

---

## Tradeoffs d’ingénierie

- **Spark vs pandas** : Spark = scale + GCP parity, pandas = simple mais moins portable.
- **Serverless vs clusters** : serverless réduit l’ops, mais cold starts plus longs.
- **Dataset public** : facile à reproduire, moins flexible pour données privées.
- **Dashboard pré‑calculé** : cohérent et rapide, pas temps réel.

---

## Limites & prochaines étapes

**Limites**
- Modèle baseline uniquement
- Pas de tracking d’expériences
- Dashboard statique (agrégats)

**Next steps**
- MLflow + registry
- CI de validation des outputs
- Runs Dataproc planifiés + monitoring

---

## Exécution locale

```bash
make setup
./scripts/download_data.sh
make rerun_all_force
make dashboard
```

---

## Exécution GCP (Dataproc Serverless)

```bash
make gcp_auth_check
make gcp_setup
make gcp_upload_raw
make gcp_package
make gcp_run_all
```
