"""Model training script."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

try:  # optional ML dependencies
    from joblib import dump
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import precision_score, recall_score
except Exception as exc:  # pragma: no cover - handled at runtime
    print(f"[train] Missing ML deps: {exc}")
    sys.exit(0)

import gspread
from gspread_dataframe import get_as_dataframe

from . import features


def main() -> int:
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id:
        print("[train] Missing GOOGLE_SHEET_ID; skipping")
        return 0

    gc = gspread.service_account(filename="service_account.json")
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("Leads")
    df = get_as_dataframe(ws, evaluate_formulas=True).dropna(how="all")
    df = df[df["Verdict"].isin(["Keep", "Drop"])]

    if df.empty:
        print("[train] No labels; skipping")
        return 0

    df["label"] = (df["Verdict"] == "Keep").astype(int)
    X = features.build_features(df)
    y = df["label"].to_numpy()

    model = LogisticRegression(max_iter=1000)
    model.fit(X, y)
    preds = model.predict(X)
    precision = precision_score(y, preds, zero_division=0)
    recall = recall_score(y, preds, zero_division=0)
    print(f"[train] precision={precision:.3f} recall={recall:.3f}")

    model_dir = Path("model")
    model_dir.mkdir(exist_ok=True)
    dump(model, model_dir / "model.joblib")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
