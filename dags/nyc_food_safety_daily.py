"""
nyc_food_safety_daily.py  —  the one Airflow job (DAG) for Phase 1
Repo location: dags/nyc_food_safety_daily.py

Runs every day at ~7am Eastern:
    1. inspections     -> nyc_restaurants.raw_inspections      (full pull, merge)
    2. complaints_311  -> nyc_restaurants.raw_311_complaints   (last 30 days, merge)
    3. dbt build       -> nyc_analytics                        (models and tests)

Tasks run one after another (not in parallel) to stay within the worker's 2 GB memory.
No catchup needed: inspections is a full pull; 311 back-fills itself on its first run.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="nyc_food_safety_daily",
    schedule="0 11 * * *",             # 11:00 UTC = 7am Eastern
    start_date=datetime(2026, 10, 20),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ba882", "phase1"],
)
def nyc_food_safety_daily():

    @task
    def inspections():
        from include.extract.nyc_inspections import run_inspections
        return run_inspections()

    @task
    def complaints_311():
        from include.extract.nyc_311 import run_311
        return run_311()

    @task(queue="dbt")
    def dbt_build():
        from include.dbt_runner import run_dbt_build
        return run_dbt_build()

    inspections() >> complaints_311() >> dbt_build()


nyc_food_safety_daily()
