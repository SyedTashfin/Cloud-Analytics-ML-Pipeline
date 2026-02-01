# Security

## Data handling
- The default dataset is **public** and fetched from GCS.
- No secrets are required for local runs.
- For private datasets, authenticate with `gcloud auth login` and avoid committing any credentials.

## Secrets policy
- Do not commit service account keys, API tokens, or private datasets.

## Reporting
If you discover a security issue, please open a GitHub issue with a minimal reproduction and mark it as security‑related.
