# Phase 1 Foundation Handoff

## Narrative

Where is food-safety risk concentrated and rising across NYC,
and which restaurants are most likely to receive poor
inspection outcomes?

These models support descriptive reporting and historical
indicators. Prediction probabilities are not implemented.

## Implemented models

| Model | Materialization | Grain |
|---|---|---|
| stg_311_complaints | View | One cleaned service request |
| fct_311_complaints | Incremental MERGE | One service request |
| stg_inspections | View | One source citation-level record |
| dim_restaurants | Table | One restaurant permit/CAMIS |
| fct_inspections | Table | One reconstructed inspection |
| fct_violations | Table | One coded violation citation |

Field definitions and tests are in:
- models/staging/stg_311_complaints.yml
- models/core/fct_311_complaints.yml
- models/inspection_models.yml

SQL files provide the exact output columns.
The complaint fact inherits staging fields and adds
month_start and is_food_poisoning.

## Validation observed on October 23, 2026

- 60,569 unique complaints.
- 48,743 Food Establishment complaints.
- 11,826 Food Poisoning complaints.
- 31,443 unique restaurant permits.
- 92,983 unique reconstructed inspections.
- 39 data tests passed across the two model builds.
- Complaint incremental MERGE succeeded.
- Complaint row count remained 60,569 after the MERGE.

## Rules for the marts

- Use ref() to reference the foundation models.
- Aggregate inspections and complaints separately before joining.
- Join geographic aggregates by zip_code and calendar month.
- Food Poisoning is included in total food complaints;
  do not add the subset to the total.
- Exclude null ZIPs from ZIP-level reporting and disclose coverage.
- Of the observed complaints, 799 have no usable ZIP and
  666 have no recognized borough.
- There are 1,476 inspections with unknown critical outcomes.
  Exclude null outcomes from known-outcome rate denominators.
- Calculate grade percentages among eligible A/B/C grades.
- Keep grade-pending and missing grades separate.
- Compare consistent inspection types and disclose denominators.
- Restaurant geography is the latest observed location,
  not a verified historical address.
- Restaurant permits are not guaranteed to be currently active.
- Retain older inspection history, but use a documented common
  reporting window when comparing with complaints from 2023 onward.
- Flag incomplete months and distinguish missing data from zero.
- ZIP-level complaints are context, not restaurant attribution.

## Refresh and deployment status

Latest observed raw extraction timestamps were October 22, 2026.
Model build time does not establish source freshness.

Current outputs are in dbt_dev_hamza.
Production publishing to nyc_analytics is pending.
The deployed Airflow DAG does not yet execute dbt.

## Three-table pipeline requirement

The existing scheduled ingestion updates:
1. nyc_restaurants.raw_inspections
2. nyc_restaurants.raw_311_complaints

The intended third table is:
3. nyc_analytics.fct_311_complaints

Its incremental MERGE is tested in development.
The requirement remains pending until production integration
and a successful scheduled pipeline run are verified.
