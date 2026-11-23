"""
nyc_food_safety_cosmos_test.py  -  TEST DAG: runs the dbt project through Cosmos.

Repo location: dags/nyc_food_safety_cosmos_test.py

- Manual trigger only (schedule=None). The production DAG is untouched.
- Writes to a SCRATCH dataset (default: nyc_analytics_cosmos_test), never nyc_analytics.
- One task at a time (max_active_tasks=1) on the "dbt" worker queue, to stay within
  the worker's 2 GB of memory.
- dbt is already installed in the Airflow image (requirements.txt), so ExecutionMode.LOCAL
  needs no virtualenv.
"""
import os
from datetime import datetime
from pathlib import Path

from cosmos import (
    DbtDag,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, TestBehavior
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

# Same project folder the production runner uses (dags/ -> repo root -> dbt/nyc_food_safety)
DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt" / "nyc_food_safety"

# Airflow connection holding the service-account key (create it before triggering).
GCP_CONN_ID = os.getenv("DBT_GCP_CONN_ID", "gcp_dbt")
SCRATCH_DATASET = os.getenv("DBT_COSMOS_DATASET", "nyc_analytics_cosmos_test")

profile_config = ProfileConfig(
    profile_name="nyc_food_safety",
    target_name="cosmos_test",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id=GCP_CONN_ID,
        profile_args={
            "project": os.environ["GCP_PROJECT_ID"].strip(),
            "dataset": SCRATCH_DATASET,
            "location": "US",
            "threads": 1,
        },
    ),
)

nyc_food_safety_cosmos_test = DbtDag(
    dag_id="nyc_food_safety_cosmos_test",
    project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
    profile_config=profile_config,
    execution_config=ExecutionConfig(execution_mode=ExecutionMode.LOCAL),
    render_config=RenderConfig(
        test_behavior=TestBehavior.AFTER_EACH,   # model, then its tests
        exclude=["setup_check"],                 # same as --exclude setup_check today
    ),
    operator_args={"queue": "dbt"},              # keep the dbt worker routing
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_tasks=1,                          # memory: one dbt process at a time
    default_args={"retries": 1},
    tags=["ba882", "cosmos", "test"],
)
