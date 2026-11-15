# Migration steps (run from the dbt project root)

## 0. Snapshot current outputs (on main, after a fresh build, same day)
```sql
create schema if not exists `PROJECT.dbt_validation`;
create or replace table `PROJECT.dbt_validation.dim_restaurants_old`     as select * from `PROJECT.DATASET.dim_restaurants`;
create or replace table `PROJECT.dbt_validation.fct_inspections_old`     as select * from `PROJECT.DATASET.fct_inspections`;
create or replace table `PROJECT.dbt_validation.fct_violations_old`      as select * from `PROJECT.DATASET.fct_violations`;
create or replace table `PROJECT.dbt_validation.mart_restaurant_risk_old` as select * from `PROJECT.DATASET.mart_restaurant_risk`;
```

## 1. Move files (new branch)
```bash
git checkout -b refactor/intermediate-layer
mkdir -p models/intermediate models/marts/reporting
git mv models/core models/marts/core
git mv models/marts/mart_restaurant_risk.sql models/marts/reporting/
git mv models/marts/mart_zip_monthly.sql     models/marts/reporting/
git mv models/marts/marts.yml                models/marts/reporting/
git mv models/inspection_models.yml          models/marts/core/inspection_models.yml
```
(Run the `core` move before the `marts` file moves; if `models/marts/` already
exists, `git mv models/core models/marts/core` nests correctly.)

## 2. Split the YAML
Cut the `- name: stg_inspections` block (everything above `- name: dim_restaurants`)
out of `models/marts/core/inspection_models.yml` into
`models/staging/stg_inspections.yml`, headed by `version: 2` and `models:`.

## 3. Drop in the new files
Copy from this folder: dbt_project.yml, macros/as_of_date.sql,
models/intermediate/*, and overwrite dim_restaurants.sql, fct_inspections.sql,
fct_violations.sql, mart_restaurant_risk.sql.

## 4. Build and test
```bash
dbt parse
dbt build
```

## 5. Compare old vs new (should return 0 / 0 for every table)
```sql
select 'old_minus_new' as direction, count(*) as n from (
  select * from `PROJECT.dbt_validation.fct_inspections_old`
  except distinct
  select * from `PROJECT.DATASET.fct_inspections`)
union all
select 'new_minus_old', count(*) from (
  select * from `PROJECT.DATASET.fct_inspections`
  except distinct
  select * from `PROJECT.dbt_validation.fct_inspections_old`);
```
Repeat for dim_restaurants and fct_violations. For the mart use
`select * except (built_at)` on both sides.
