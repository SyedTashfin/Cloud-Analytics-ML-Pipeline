# Final Report: E-Commerce Clickstream Analysis

## Introduction

This report summarizes a reproducible PySpark pipeline for batch processing and
session-level purchase prediction on an e-commerce clickstream dataset. The goal is
to transform raw event logs into auditable parquet outputs, engineer session-level
features, and evaluate a baseline classification model.

## Dataset

- Input rows: 109,950,743
- Output rows after cleaning: 109,950,743
- Distinct users: 5,316,649
- Distinct sessions: 23,016,650
- Event time range: 2019-10-01 02:00:00 to 2019-12-01 00:59:59

The dataset includes event-level fields used for processing and feature generation:
`event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`,
`price`, `user_id`, and `user_session`.

## Methodology

The pipeline reads raw CSVs with an explicit schema, normalizes timestamps, filters
invalid records, and writes partitioned parquet outputs. Data integrity checks are
recorded in JSON summaries. Session-level features are aggregated from the cleaned
events data and stored as a separate parquet dataset. A logistic regression model is
trained on session features and evaluated on a random train/test split.

## Batch Processing

Events are written to `data/processed/events_parquet/` partitioned by `event_date`.
On local runs, the ingestion step writes in date chunks (default: 7 days per chunk)
to reduce memory pressure. Parquet outputs use snappy compression for efficient
storage and scan performance.

## Feature Engineering

Session-level features are aggregated using `user_session` as the grouping key:

- total_events
- num_views
- num_carts
- num_purchases
- unique_products
- unique_categories
- session_duration_seconds
- avg_price_viewed
- max_price_viewed
- hour_of_day
- day_of_week
- is_weekend

The label `purchase_in_session` equals 1 if any purchase occurs within a session.

- Session rows: 23,016,650
- Positive rate: 0.060945

Features are written to `data/processed/session_features/`.

## Machine Learning Analysis

A logistic regression model is trained using the engineered session-level numeric
features. The pipeline applies a random train/test split of 0.8/0.2 and uses a
threshold of 0.5 for classification. Model settings follow the configuration:
`max_iter=50`, `reg_param=0.01`, `elastic_net_param=0.0`.

## Visualization & Results

The primary metrics are summarized in `reports/metrics.json` and visualized in
`reports/figures/metrics.png`.

- AUC: 0.999973
- Accuracy: 0.999938
- Precision: 0.998985
- Recall: 1.0
- Positive rate: 0.060896

## Conclusions

The pipeline successfully transforms large-scale clickstream logs into auditable
parquet datasets and produces a strong baseline model on session-level features.
The results indicate high separability for purchase sessions under the current
feature set and evaluation protocol.

## Limitations

- The analysis covers only the available months in the raw dataset.
- The label is defined at the session level and does not capture longer-term user
  behavior.
- The evaluation uses a random split rather than a time-based split, which may
  overestimate performance for future periods.
- Feature engineering is limited to session-level aggregates and excludes
  additional behavioral or inventory context.
