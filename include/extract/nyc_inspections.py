"""
nyc_inspections.py  —  Extract + load NYC Restaurant Inspections into BigQuery
Repo location: include/extract/nyc_inspections.py

Plain language:
  1. Ask NYC Open Data for the WHOLE inspections dataset (about 300K rows), page by page.
  2. Load it into a temporary BigQuery table.
  3. MERGE it into raw_inspections:
        - rows we have never seen  -> INSERTED  (new inspections)
        - rows we already have     -> UPDATED   (e.g. a score changed after appeal)
     That is the "add/update records" requirement, done safely: re-running never duplicates.

Why pull everything daily instead of "only new rows"?
  The city revises old rows (scores change after appeals), so "only new" would miss changes.
  The dataset is small, so a full pull takes a minute or two.

Env vars (set in Astronomer -> Deployment -> Environment, NEVER in GitHub):
  GCP_PROJECT_ID      your project id
  BQ_DATASET          e.g. "raw"
  GCP_SA_KEY_JSON     full JSON text of the service-account key (mark as secret)
  SOCRATA_APP_TOKEN   optional; free token from data.cityofnewyork.us, avoids throttling
"""

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
PAGE_SIZE = 50_000
TABLE = "raw_inspections"

# One row in this dataset = one violation cited at one inspection.
# These columns together identify a row (no-violation inspections have a blank code).
KEY_COLS = ["camis", "inspection_date", "inspection_type", "violation_code"]


# ---------------------------------------------------------------------------
# 1. EXTRACT
# ---------------------------------------------------------------------------
def extract_inspections() -> pd.DataFrame:
    headers = {}
    if os.getenv("SOCRATA_APP_TOKEN"):
        headers["X-App-Token"] = os.environ["SOCRATA_APP_TOKEN"]

    pages, offset = [], 0
    while True:
        params = {"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"}  # stable order = no gaps
        for attempt in range(1, 4):
            resp = requests.get(URL, params=params, headers=headers, timeout=120)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(10 * attempt)
                continue
            resp.raise_for_status()
            break
        else:
            raise RuntimeError(f"NYC API failed at offset {offset}")

        batch = resp.json()
        if not batch:
            break
        pages.append(pd.DataFrame(batch))
        print(f"fetched {offset + len(batch):,} rows")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    df = pd.concat(pages, ignore_index=True) if pages else pd.DataFrame()
    return clean(df)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Make the table BigQuery-safe. Real cleaning (types, dates) happens later in dbt."""
    # drop Socrata system/computed columns like ':@computed_region_xyz'
    df = df[[c for c in df.columns if not c.startswith((":", "@"))]].copy()
    # make sure key columns exist even if a page had none of them
    for c in KEY_COLS:
        if c not in df.columns:
            df[c] = None
    # store everything as text in RAW (avoids schema errors); dbt casts types later
    df = df.astype("string").where(df.notna(), None)
    # build one id per row from the key columns
    df["row_key"] = df[KEY_COLS].fillna("").agg("|".join, axis=1)
    # if the API returns exact duplicates of a key, keep one
    df = df.drop_duplicates(subset="row_key", keep="last")
    df["extracted_at"] = datetime.now(timezone.utc)
    return df


# ---------------------------------------------------------------------------
# 2. LOAD (temp table -> MERGE)
# ---------------------------------------------------------------------------
def _clean_env(name: str) -> str:
    """Read an env var and fix invisible copy-paste problems (spaces, look-alike hyphens)."""
    return os.environ[name].strip().replace("\u2011", "-").replace("\u2010", "-").replace("\u2013", "-")


def _bq_client():
    from google.cloud import bigquery
    from google.oauth2 import service_account

    project = _clean_env("GCP_PROJECT_ID")
    key_json = os.getenv("GCP_SA_KEY_JSON")
    if key_json:
        creds = service_account.Credentials.from_service_account_info(json.loads(key_json))
        return bigquery.Client(project=project, credentials=creds)
    return bigquery.Client(project=project)  # falls back to default login (e.g. Cloud Shell)


def load_inspections(df: pd.DataFrame) -> dict:
    from google.cloud import bigquery

    client = _bq_client()
    ds = f"{_clean_env('GCP_PROJECT_ID')}.{_clean_env('BQ_DATASET')}"
    target, temp = f"{ds}.{TABLE}", f"{ds}._tmp_{TABLE}"

    # a) load today's pull into a temp table (replaced every run)
    client.load_table_from_dataframe(
        df, temp, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    ).result()

    # b) first run ever: create the target with the same columns
    client.query(f"CREATE TABLE IF NOT EXISTS `{target}` AS SELECT * FROM `{temp}` WHERE FALSE").result()

    # c) merge: update rows we have, insert rows we don't
    cols = [c for c in df.columns if c != "row_key"]
    set_clause = ", ".join(f"T.`{c}` = S.`{c}`" for c in cols)
    all_cols = ", ".join(f"`{c}`" for c in df.columns)
    merge = client.query(f"""
        MERGE `{target}` T
        USING `{temp}` S
        ON T.row_key = S.row_key
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({all_cols})
    """)
    merge.result()
    client.delete_table(temp, not_found_ok=True)

    stats = {"rows_pulled": len(df), "rows_changed": merge.num_dml_affected_rows}
    print(stats)
    return stats


def run_inspections() -> dict:
    """The one function Airflow calls."""
    return load_inspections(extract_inspections())


if __name__ == "__main__":
    # Local/Cloud Shell test: pull and show, no BigQuery write
    d = extract_inspections()
    print(d.shape)
    print(d.head(3).T)
