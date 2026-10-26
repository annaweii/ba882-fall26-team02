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
def _api_count(headers):
    """Count source rows independently of pagination."""
    response = requests.get(
        URL,
        params={"$select": "count(*)"},
        headers=headers,
        timeout=120,
    )
    response.raise_for_status()
    return int(response.json()[0]["count"])


def extract_inspections() -> pd.DataFrame:
    headers = {}
    if os.getenv("SOCRATA_APP_TOKEN"):
        headers["X-App-Token"] = os.environ["SOCRATA_APP_TOKEN"]

    expected_before = _api_count(headers)
    pages, offset = [], 0

    while True:
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": ":id",
        }

        for attempt in range(1, 4):
            try:
                resp = requests.get(
                    URL, params=params, headers=headers, timeout=120
                )
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.RequestException(
                        f"Retryable HTTP {resp.status_code}"
                    )
                resp.raise_for_status()
                batch = resp.json()
                break
            except (requests.RequestException, ValueError):
                if attempt == 3:
                    raise
                time.sleep(10 * attempt)

        if not isinstance(batch, list):
            raise ValueError("NYC API returned a non-list page")

        if not batch:
            break

        pages.append(pd.DataFrame(batch))
        offset += len(batch)
        print(f"Fetched {offset:,} inspection rows")

        if len(batch) < PAGE_SIZE:
            break

    downloaded = sum(len(page) for page in pages)
    expected_after = _api_count(headers)

    if min(expected_before, expected_after) < 200_000:
        raise ValueError("NYC API snapshot count unexpectedly small")

    for expected in (expected_before, expected_after):
        difference = abs(downloaded - expected) / expected
        if difference > 0.01:
            raise ValueError(
                f"Incomplete snapshot: downloaded={downloaded}, "
                f"API count={expected}, difference={difference:.2%}"
            )

    raw = pd.concat(pages, ignore_index=True)
    df = clean(raw)

    if len(df) < 200_000 or df["row_key"].isna().any():
        raise ValueError("Cleaned inspection snapshot failed validation")

    if not df["row_key"].is_unique:
        raise ValueError("Duplicate inspection row keys remain")

    print(
        f"Snapshot validated: downloaded={downloaded:,}, "
        f"cleaned={len(df):,}, API count={expected_after:,}"
    )
    df.attrs["snapshot_validated"] = True
    df.attrs["api_count"] = expected_after
    df.attrs["downloaded_count"] = downloaded
    df.attrs["validated_row_count"] = len(df)
    return df


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

    # Keep all new archival/deletion code isolated during development.
    if _clean_env("BQ_DATASET") != "dbt_integration_test_shanmati":
        raise ValueError(
            "Archival development code is restricted to the test dataset"
        )

    if not df.attrs.get("snapshot_validated"):
        raise ValueError(
            "Refusing deletion-enabled load: API snapshot not validated"
        )

    if len(df) != df.attrs.get("validated_row_count"):
        raise ValueError(
            "Snapshot row count changed after extraction validation"
        )

    if len(df) < 200_000:
        raise ValueError("Refusing to synchronize a small snapshot")

    if df["row_key"].isna().any() or not df["row_key"].is_unique:
        raise ValueError("Invalid inspection row keys")

    client = _bq_client()
    ds = (
        f"{_clean_env('GCP_PROJECT_ID')}."
        f"{_clean_env('BQ_DATASET')}"
    )
    target = f"{ds}.{TABLE}"
    temp = f"{ds}._tmp_{TABLE}"
    history = f"{ds}.{TABLE}_history"

    columns = list(df.columns)
    business_columns = [
        col for col in columns
        if col not in ("row_key", "extracted_at")
    ]

    def row_hash(alias):
        fields = ", ".join(
            f"{alias}.`{col}` AS `{col}`"
            for col in business_columns
        )
        return f"FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({fields})))"

    changed = f"({row_hash('T')} != {row_hash('S')})"

    all_cols = ", ".join(f"`{col}`" for col in columns)
    insert_values = ", ".join(f"S.`{col}`" for col in columns)
    update_clause = ", ".join(
        f"T.`{col}` = S.`{col}`"
        for col in columns if col != "row_key"
    )

    client.load_table_from_dataframe(
        df,
        temp,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        ),
    ).result()

    try:
        # Read-only preflight. The production version will additionally
        # require a verified production snapshot backup.
        stats_sql = f"""
        SELECT
          COUNTIF(S.row_key IS NULL) AS deleted,
          COUNTIF(S.row_key IS NOT NULL AND {changed}) AS updated
        FROM `{target}` T
        LEFT JOIN `{temp}` S USING (row_key)
        """
        counts = next(iter(client.query(stats_sql).result()))
        deleted = int(counts.deleted)
        updated = int(counts.updated)

        existing = next(iter(client.query(
            f"SELECT COUNT(*) AS n FROM `{target}`"
        ).result())).n

        if existing < 200_000:
            raise ValueError("Existing inspection table too small")

        if deleted > existing * 0.02:
            raise ValueError(
                f"Deletion safety limit exceeded: "
                f"{deleted:,} of {existing:,} rows"
            )

        inserted = next(iter(client.query(f"""
            SELECT COUNT(*) AS n
            FROM `{temp}` S
            LEFT JOIN `{target}` T USING (row_key)
            WHERE T.row_key IS NULL
        """).result())).n

        print(
            f"Preflight: existing={existing:,}, "
            f"inserted={inserted:,}, updated={updated:,}, "
            f"deleted={deleted:,}"
        )

        # Both history INSERT and current-table MERGE are atomic.
        transaction_sql = f"""
        BEGIN
        BEGIN TRANSACTION;

        ASSERT (
          (SELECT COUNT(*) FROM `{temp}`) >= 200000
        ) AS 'Source snapshot unexpectedly small';

        ASSERT (
          (SELECT COUNT(*)
           FROM `{target}` T
           LEFT JOIN `{temp}` S USING (row_key)
           WHERE S.row_key IS NULL)
          <= (SELECT COUNT(*) * 0.02 FROM `{target}`)
        ) AS 'Deletion limit exceeded';

        INSERT INTO `{history}`
        ({all_cols}, change_type, archived_at)
        SELECT
          {", ".join(f"T.`{col}`" for col in columns)},
          IF(S.row_key IS NULL, 'deleted', 'updated'),
          CURRENT_TIMESTAMP()
        FROM `{target}` T
        LEFT JOIN `{temp}` S USING (row_key)
        WHERE S.row_key IS NULL
           OR (S.row_key IS NOT NULL AND {changed});

        MERGE `{target}` T
        USING `{temp}` S
        ON T.row_key = S.row_key
        WHEN MATCHED AND {changed}
          THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED BY TARGET
          THEN INSERT ({all_cols}) VALUES ({insert_values})
        WHEN NOT MATCHED BY SOURCE
          THEN DELETE;

        COMMIT TRANSACTION;
        EXCEPTION WHEN ERROR THEN
          ROLLBACK TRANSACTION;
          RAISE;
        END;
        """

        client.query(transaction_sql).result()

        stats = {
            "rows_pulled": len(df),
            "inserted": int(inserted),
            "updated": updated,
            "deleted": deleted,
            "archived": updated + deleted,
        }
        print(stats)
        return stats

    finally:
        client.delete_table(temp, not_found_ok=True)


def run_inspections() -> dict:
    """The one function Airflow calls."""
    return load_inspections(extract_inspections())


if __name__ == "__main__":
    # Local/Cloud Shell test: pull and show, no BigQuery write
    d = extract_inspections()
    print(d.shape)
    print(d.head(3).T)
