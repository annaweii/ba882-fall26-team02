"""
nyc_311.py  —  Extract + load NYC 311 FOOD-RELATED complaints into BigQuery
Repo location: include/extract/nyc_311.py

Plain language:
  - 311 is huge (millions of complaints), so we only ask for food-related types.
  - FIRST run (table doesn't exist yet): pull everything since 2023-01-01
    (matches the ~3-year window of the inspections data).
  - EVERY run after: pull only the last 30 days. Recent complaints still change
    (status, closed date, resolution), so we MERGE: update existing, insert new.

Uses the same env vars as nyc_inspections.py.
"""

import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

from include.extract.nyc_inspections import _bq_client, _clean_env  # reuse the same BigQuery login

URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
TABLE = "raw_311_complaints"
PAGE_SIZE = 50_000
FIRST_LOAD_START = "2023-01-01T00:00:00"
LOOKBACK_DAYS = 30

COMPLAINT_TYPES = ["Food Poisoning", "Food Establishment"]

# keep only the columns we need (311 has ~40)
COLUMNS = [
    "unique_key", "created_date", "closed_date", "agency", "complaint_type",
    "descriptor", "location_type", "incident_zip", "incident_address",
    "street_name", "city", "borough", "status", "resolution_description",
    "latitude", "longitude",
]


# ---------------------------------------------------------------------------
# 1. EXTRACT
# ---------------------------------------------------------------------------
def extract_311(since_iso: str) -> pd.DataFrame:
    import os
    headers = {}
    if os.getenv("SOCRATA_APP_TOKEN"):
        headers["X-App-Token"] = os.environ["SOCRATA_APP_TOKEN"]

    types = ",".join(f"'{t}'" for t in COMPLAINT_TYPES)
    where = f"complaint_type in({types}) AND created_date >= '{since_iso}'"

    pages, offset = [], 0
    while True:
        params = {
            "$select": ",".join(COLUMNS),
            "$where": where,
            "$order": "unique_key",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        for attempt in range(1, 4):
            resp = requests.get(URL, params=params, headers=headers, timeout=120)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(10 * attempt)
                continue
            resp.raise_for_status()
            break
        else:
            raise RuntimeError(f"311 API failed at offset {offset}")

        batch = resp.json()
        if not batch:
            break
        pages.append(pd.DataFrame(batch))
        print(f"fetched {offset + len(batch):,} complaints")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    df = pd.concat(pages, ignore_index=True) if pages else pd.DataFrame(columns=COLUMNS)
    return clean(df)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    for c in COLUMNS:                      # make sure every column exists
        if c not in df.columns:
            df[c] = None
    df = df[COLUMNS].astype("string").where(df[COLUMNS].notna(), None)
    df = df[df["unique_key"].notna()].drop_duplicates(subset="unique_key", keep="last")
    df["extracted_at"] = datetime.now(timezone.utc)
    return df


# ---------------------------------------------------------------------------
# 2. LOAD (temp table -> MERGE on unique_key)
# ---------------------------------------------------------------------------
def _table_exists(client, table_id: str) -> bool:
    from google.api_core.exceptions import NotFound
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def load_311(df: pd.DataFrame, client, target: str, temp: str) -> dict:
    from google.cloud import bigquery

    if df.empty:
        print("no complaints returned")
        return {"rows_pulled": 0, "rows_changed": 0}

    client.load_table_from_dataframe(
        df, temp, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    ).result()
    client.query(f"CREATE TABLE IF NOT EXISTS `{target}` AS SELECT * FROM `{temp}` WHERE FALSE").result()

    cols = list(df.columns)
    set_clause = ", ".join(f"T.`{c}` = S.`{c}`" for c in cols if c != "unique_key")
    all_cols = ", ".join(f"`{c}`" for c in cols)
    merge = client.query(f"""
        MERGE `{target}` T
        USING `{temp}` S
        ON T.unique_key = S.unique_key
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({all_cols})
    """)
    merge.result()
    client.delete_table(temp, not_found_ok=True)

    stats = {"rows_pulled": len(df), "rows_changed": merge.num_dml_affected_rows}
    print(stats)
    return stats


def run_311() -> dict:
    """The one function Airflow calls."""
    import os
    client = _bq_client()
    ds = f"{_clean_env('GCP_PROJECT_ID')}.{_clean_env('BQ_DATASET')}"
    target, temp = f"{ds}.{TABLE}", f"{ds}._tmp_{TABLE}"

    if _table_exists(client, target):
        since = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT00:00:00")
        print(f"incremental run: complaints since {since}")
    else:
        since = FIRST_LOAD_START
        print(f"first run: full history since {since}")

    return load_311(extract_311(since), client, target, temp)


if __name__ == "__main__":
    d = extract_311((datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00"))
    print(d.shape)
    print(d.head(3).T)
