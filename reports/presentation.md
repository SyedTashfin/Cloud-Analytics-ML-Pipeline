# Presentation Outline (15 Minutes)

## Slide 1: Title and Objectives (1:00)
- E-commerce Clickstream Analytics with PySpark
- Goal: auditable ingestion, session features, and purchase prediction

Speaker notes: Introduce the project scope and the end-to-end pipeline objective.

## Slide 2: Dataset Overview (1:15)
- 109,950,743 input events
- 5,316,649 distinct users
- 23,016,650 distinct sessions
- Event time range: 2019-10-01 02:00:00 to 2019-12-01 00:59:59

Speaker notes: Emphasize scale and temporal coverage to motivate batch processing.

## Slide 3: Pipeline Architecture (1:15)
- Raw CSV -> events parquet -> session features -> model -> metrics
- Config-driven paths and parameters
- Auditable JSON summaries

Speaker notes: Explain the staged design and why each stage is separated.

## Slide 4: Ingestion and Cleaning (1:30)
- Explicit schema, timestamp normalization, and invalid-row filtering
- Partitioned parquet by event_date
- Snappy compression

Speaker notes: Highlight data quality rules and output guarantees.

## Slide 5: Batch Processing Strategy (1:30)
- Local chunked writes (max 7 days per chunk)
- Avoids memory pressure on laptops
- Deterministic outputs and safe overwrites

Speaker notes: Describe why chunking is essential for local stability.

## Slide 6: Session Feature Engineering (2:00)
- Aggregation over user_session
- Behavioral counts (views, carts, purchases)
- Diversity (unique products, categories)
- Temporal features (hour, day, weekend)

Speaker notes: Walk through the feature family and its intent.

## Slide 7: Label Definition (0:45)
- purchase_in_session = 1 if any purchase occurs
- Positive rate: 0.060945

Speaker notes: Note class imbalance and why label is session-scoped.

## Slide 8: Modeling Approach (1:30)
- Logistic regression on numeric session features
- 0.8/0.2 train/test split
- Threshold = 0.5

Speaker notes: Keep the model simple and explain baseline purpose.

## Slide 9: Results Summary (2:00)
- AUC: 0.999973
- Accuracy: 0.999938
- Precision: 0.998985
- Recall: 1.0
- Positive rate: 0.060896

Speaker notes: Report the metrics and interpret overall performance.

## Slide 10: Visualization (1:00)
- Metrics bar chart: reports/figures/metrics.png

Speaker notes: Show the figure and connect it to numeric metrics.

## Slide 11: Conclusions and Limitations (2:05)
- Pipeline delivers reproducible, auditable results
- Strong baseline performance on session features
- Limitations: time split, session-only features, limited months

Speaker notes: Summarize outcomes and identify next steps for robustness.
