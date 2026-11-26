"""
ml-train-inspection  -  Cloud Function that trains one model for the
NYC restaurant risk project (model-inspection-risk).

Called by the Airflow training DAG (one call per algorithm, in parallel).
- Reads the dbt-owned feature table nyc_analytics.ml_inspection_features
- Time-based split: trains on older inspections, tests on the latest 6 months
- Trains the requested algorithm, computes metrics
- Saves the fitted pipeline to GCS and returns metrics + artifact path

No feature engineering lives here: dbt owns the data layer (Lab 9 rule).
"""
import json
import os
import pickle
from datetime import datetime, timezone

import functions_framework
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

# ---------------------------------------------------------------- settings
PROJECT = os.environ.get("GCP_PROJECT_ID", "sambisha-ba882-fall26").strip()
BUCKET = os.environ.get("MODEL_BUCKET", "sambisha-ba882-fall26-ml-models").strip()
MODEL_ID = "model-inspection-risk"
FEATURE_TABLE = f"{PROJECT}.nyc_analytics.ml_inspection_features"
LABEL = "label_not_a"
TEST_MONTHS = 6

# Same column names as mart_restaurant_risk, so the saved model can score it.
NUMERIC_FEATURES = [
    "days_since_last_inspection",
    "last_inspection_score",
    "inspection_count_12m",
    "average_inspection_score_12m",
    "inspections_with_critical_count_12m",
    "critical_violation_count_12m",
    "zip_food_complaint_count_90d",
]
CATEGORICAL_FEATURES = ["borough", "cuisine_description", "last_graded_grade"]
FLAG_FEATURES = ["is_first_inspection"]  # derived below: no earlier inspection

ALGORITHMS = {
    "logistic_regression": LogisticRegression,
    "random_forest": RandomForestClassifier,
    "gradient_boosting": GradientBoostingClassifier,
}


# ---------------------------------------------------------------- helpers
def load_features() -> pd.DataFrame:
    """Read the feature table from BigQuery."""
    from google.cloud import bigquery

    columns = ", ".join(["inspection_date", LABEL] + NUMERIC_FEATURES + CATEGORICAL_FEATURES)
    sql = f"SELECT {columns} FROM `{FEATURE_TABLE}`"
    return bigquery.Client(project=PROJECT).query(sql).to_dataframe()


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Light, model-side typing only (no business logic)."""
    df = df.copy()
    df["inspection_date"] = pd.to_datetime(df["inspection_date"])
    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    for col in CATEGORICAL_FEATURES:
        df[col] = df[col].astype("object").where(df[col].notna(), "UNKNOWN").astype(str)
    df["is_first_inspection"] = df["last_inspection_score"].isna().astype(int)
    df[LABEL] = df[LABEL].astype(int)
    return df


def time_split(df: pd.DataFrame):
    """Train on older inspections, test on the most recent TEST_MONTHS."""
    cutoff = df["inspection_date"].max() - pd.DateOffset(months=TEST_MONTHS)
    train = df[df["inspection_date"] < cutoff]
    test = df[df["inspection_date"] >= cutoff]
    return train, test, cutoff


def build_pipeline(algorithm: str, hyperparameters: dict) -> Pipeline:
    if algorithm not in ALGORITHMS:
        raise ValueError(f"Unknown algorithm '{algorithm}'. Use one of {list(ALGORITHMS)}")

    numeric = Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
    ])
    categorical = OneHotEncoder(handle_unknown="ignore", min_frequency=50)
    preprocess = ColumnTransformer(
        [
            ("num", numeric, NUMERIC_FEATURES),
            ("cat", categorical, CATEGORICAL_FEATURES),
            ("flag", "passthrough", FLAG_FEATURES),
        ],
        sparse_threshold=0.0,  # dense output works for all three algorithms
    )
    params = dict(hyperparameters or {})
    if algorithm in ("logistic_regression", "random_forest"):
        params.setdefault("class_weight", "balanced")
    params.setdefault("random_state", 42)
    return Pipeline([("prep", preprocess), ("clf", ALGORITHMS[algorithm](**params))])


def hit_rate_top(y_true, scores, fraction=0.10) -> float:
    """Share of the riskiest `fraction` of inspections that really failed."""
    n = max(1, int(len(scores) * fraction))
    top = np.argsort(-np.asarray(scores))[:n]
    return float(np.asarray(y_true)[top].mean())


def evaluate(pipeline, test: pd.DataFrame) -> dict:
    X = test[NUMERIC_FEATURES + CATEGORICAL_FEATURES + FLAG_FEATURES]
    y = test[LABEL]
    proba = pipeline.predict_proba(X)[:, 1]
    pred = (proba >= 0.5).astype(int)
    return {
        "auc": round(float(roc_auc_score(y, proba)), 4),
        "f1": round(float(f1_score(y, pred)), 4),
        "accuracy": round(float(accuracy_score(y, pred)), 4),
        "hit_rate_top10": round(hit_rate_top(y, proba, 0.10), 4),
        "base_rate": round(float(y.mean()), 4),
    }


def save_artifact(bundle: dict, run_id: str) -> str:
    from google.cloud import storage

    path = f"{MODEL_ID}/{run_id}/model.pkl"
    blob = storage.Client(project=PROJECT).bucket(BUCKET).blob(path)
    blob.upload_from_string(pickle.dumps(bundle), content_type="application/octet-stream")
    return f"gs://{BUCKET}/{path}"


def train_and_save(df: pd.DataFrame, run_id: str, algorithm: str,
                   hyperparameters: dict, save=save_artifact) -> dict:
    df = prepare(df)
    train, test, cutoff = time_split(df)
    if train.empty or test.empty:
        raise ValueError("Time split produced an empty train or test set")

    pipeline = build_pipeline(algorithm, hyperparameters)
    pipeline.fit(train[NUMERIC_FEATURES + CATEGORICAL_FEATURES + FLAG_FEATURES], train[LABEL])
    metrics = evaluate(pipeline, test)
    metrics.update({
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "test_start_date": cutoff.date().isoformat(),
    })

    bundle = {
        "model": pipeline,
        "model_id": MODEL_ID,
        "run_id": run_id,
        "algorithm": algorithm,
        "numeric_features": NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "flag_features": FLAG_FEATURES,
        "label": LABEL,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics,
    }
    artifact = save(bundle, run_id)
    return {"run_id": run_id, "algorithm": algorithm, "metrics": metrics, "artifact": artifact}


# ---------------------------------------------------------------- entry point
@functions_framework.http
def train(request):
    """HTTP entry point. Body: {run_id, algorithm, hyperparameters, dataset_id}."""
    body = request.get_json(silent=True) or {}
    run_id = body.get("run_id") or f"run-{MODEL_ID}-{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
    algorithm = body.get("algorithm", "logistic_regression")
    hyperparameters = body.get("hyperparameters") or {}

    try:
        result = train_and_save(load_features(), run_id, algorithm, hyperparameters)
        result["dataset_id"] = body.get("dataset_id")
        print(json.dumps({"event": "trained", **result}))
        return (json.dumps(result), 200, {"Content-Type": "application/json"})
    except Exception as exc:  # return the reason to Airflow instead of a bare 500
        print(json.dumps({"event": "failed", "run_id": run_id, "error": str(exc)}))
        return (json.dumps({"run_id": run_id, "error": str(exc)}), 500,
                {"Content-Type": "application/json"})
