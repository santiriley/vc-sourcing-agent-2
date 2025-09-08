import os
import pathlib
import sys
from typing import Tuple

import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_score, recall_score
from sklearn.model_selection import train_test_split

import gspread
from gspread_dataframe import get_as_dataframe

from .features import build_feature_matrix


SHEET_NAME = "Leads"
MODEL_PATH = pathlib.Path("model/model.joblib")


def _load_labeled_rows(sheet_id: str) -> pd.DataFrame:
    """Fetch rows with a verdict label from Google Sheets."""
    gc = gspread.service_account(filename="service_account.json")
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(SHEET_NAME)
    df = get_as_dataframe(ws, evaluate_formulas=True)
    verdict = df.get("Verdict")
    if verdict is None:
        return pd.DataFrame()
    df = df[verdict.isin(["Keep", "Drop"])]
    return df


def main() -> int:
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        print("GOOGLE_SHEET_ID not set; skipping training.")
        return 0

    if not pathlib.Path("service_account.json").exists():
        print("service_account.json not found; skipping training.")
        return 0

    labeled = _load_labeled_rows(sheet_id)
    if labeled.empty:
        print("No labeled data; skipping training.")
        return 0

    y = (labeled["Verdict"] == "Keep").astype(int)
    X, meta = build_feature_matrix(labeled)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    precision = precision_score(y_test, preds, zero_division=0)
    recall = recall_score(y_test, preds, zero_division=0)
    print(f"Precision: {precision:.3f} Recall: {recall:.3f}")

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "meta": meta}, MODEL_PATH)
    print(f"Persisted model to {MODEL_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
