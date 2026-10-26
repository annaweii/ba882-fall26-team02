"""Run NYC Food Safety dbt models from the Astronomer Airflow deployment."""

import json
import os
import subprocess
import tempfile
from pathlib import Path

import yaml


DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt" / "nyc_food_safety"
PRODUCTION_DATASET = "nyc_analytics"


def run_dbt_build():
    """Build and test production dbt models after raw ingestion succeeds."""

    project = os.environ["GCP_PROJECT_ID"].strip()
    key_json = os.environ["GCP_SA_KEY_JSON"]

    # Validate the credentials without printing their contents.
    credentials = json.loads(key_json)
    if credentials.get("type") != "service_account":
        raise ValueError("Expected Google service-account credentials")

    with tempfile.TemporaryDirectory(prefix="nyc_dbt_") as directory:
        temp_dir = Path(directory)
        key_path = temp_dir / "service_account.json"
        profile_path = temp_dir / "profiles.yml"

        key_path.write_text(key_json, encoding="utf-8")
        key_path.chmod(0o600)

        profile = {
            "nyc_food_safety": {
                "target": "prod",
                "outputs": {
                    "prod": {
                        "type": "bigquery",
                        "method": "service-account",
                        "project": project,
                        "dataset": PRODUCTION_DATASET,
                        "location": "US",
                        "threads": 1,
                        "keyfile": str(key_path),
                        "job_execution_timeout_seconds": 300,
                        "job_retries": 1,
                    }
                },
            }
        }

        profile_path.write_text(
            yaml.safe_dump(profile),
            encoding="utf-8",
        )
        profile_path.chmod(0o600)

        command = [
            "dbt",
            "build",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(temp_dir),
            "--target",
            "prod",
            "--exclude",
            "setup_check",
        ]

        print("Running production dbt build in dataset:", PRODUCTION_DATASET)
        subprocess.run(command, check=True, cwd=DBT_PROJECT_DIR)
