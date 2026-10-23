# NYC Food Safety — dbt

## Business question

Where is food-safety risk concentrated and rising across NYC,
and which restaurants are most likely to receive poor
inspection outcomes?

Phase 1 supports geographic reporting and restaurant inspection
history. Predictive modeling will be added in a later phase.

## Environment

- GCP project: sambisha-ba882-fall26
- BigQuery location: US
- Raw sources: nyc_restaurants.raw_inspections
  and nyc_restaurants.raw_311_complaints
- Development outputs: dbt_dev_<name>
- Production outputs: nyc_analytics
- dbt Core: 1.12.6
- BigQuery adapter: 1.12.1

Only Airflow writes to the raw dataset.
Developers use separate development datasets.
The deployed dbt pipeline will write to nyc_analytics.
Dashboards will read from nyc_analytics.

## Ownership

Hamza owns:
- stg_inspections
- dim_restaurants
- fct_inspections
- fct_violations
- stg_311_complaints
- fct_311_complaints

Shanmati owns:
- mart_zip_monthly: one ZIP code per calendar month
- mart_restaurant_risk: one restaurant as of the reporting date

Each owner writes tests and documentation for their own models.
Upstream model code will be shared early so mart development
can proceed alongside foundation work.

## Shared conventions

- Restaurant ID: camis (STRING)
- Inspection ID: inspection_id (STRING)
- Complaint ID: complaint_id (STRING)
- ZIP: zip_code (STRING, five digits)
- Borough: borough (STRING, uppercase)
- Inspection date: inspection_date (DATE)
- Complaint creation date: complaint_created_date (DATE)
- Reporting month: month_start (DATE, first day of month)
- Critical inspection flag: has_critical_violation (BOOL)
- Extraction time: source_extracted_at (TIMESTAMP)
- Mart build time: built_at (TIMESTAMP)
- Counts: INT64
- Scores, coordinates, and rates: FLOAT64
- Rates are stored between 0 and 1.

Inspection IDs and event grouping will be validated against
the source data before finalizing the inspection model.

## Reporting rules

- Inspection source rows are citations, not separate inspections.
- Exclude 1900-01-01 from actual inspection events.
- Preserve genuine inspections with no violations.
- Keep missing and pending grades separate from A/B/C.
- Aggregate inspections and complaints separately before joining.
- Join geographic reporting by ZIP and calendar month.
- ZIP-level complaints are area context, not complaints
  attributed to individual restaurants.
- An inspection rate is null when there are no inspections.
- Flag incomplete months in trend comparisons.
- Restaurant indicators are not prediction probabilities.

## Developer setup

Clone the repository and enter dbt/nyc_food_safety.

Create and activate a personal environment:

```bash
python3 -m venv ~/.venvs/nyc-dbt
source ~/.venvs/nyc-dbt/bin/activate
python -m pip install --no-cache-dir -r requirements-dbt.txt
```

Create ~/.dbt/profiles.yml.
Shanmati's configuration is:

```yaml
nyc_food_safety:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: sambisha-ba882-fall26
      dataset: dbt_dev_shanmati
      location: US
      threads: 1
      job_execution_timeout_seconds: 300
      job_retries: 1
```

Hamza uses dataset dbt_dev_hamza.
Keep the actual profiles.yml outside the repository.

Verify setup from this dbt project directory:

```bash
dbt debug
dbt parse
dbt run --select setup_check
```

If application credentials are missing, authenticate using:

```bash
gcloud auth application-default login
```

Use the Google account with access to the team project.

## Collaboration

Use separate Git branches and development datasets.
Use source() for raw tables and ref() for upstream dbt models.
After pulling upstream model code, dbt can build dependencies
in each developer's own dataset.

## Verified status

- Hamza's BigQuery connection passed.
- Project parsing passed.
- setup_check view was created in dbt_dev_hamza.
- All six staging/core models are implemented.
- All 39 data tests passed across the complaint and inspection builds.
- fct_311_complaints uses incremental MERGE on complaint_id.
- The incremental MERGE passed its tests without duplicating records.
- Reporting marts are pending.
- dbt is not yet connected to the deployed Airflow DAG.

setup_check is a development verification model.
Exclude it from the eventual production build.
