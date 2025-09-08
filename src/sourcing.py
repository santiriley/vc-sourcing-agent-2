import pathlib
from typing import Any, Dict

import joblib
import pandas as pd
import yaml

from .features import build_feature_matrix

MODEL_PATH = pathlib.Path("model/model.joblib")


def _load_config(path: str | pathlib.Path = "config.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def score_rows(df: pd.DataFrame, config: Dict[str, Any] | None = None) -> pd.DataFrame:
    """Blend rule scores with model predictions if available."""
    config = config or _load_config()

    df = df.copy()
    for col in ["Verdict", "Reason"]:
        if col not in df.columns:
            df[col] = ""

    learning_cfg = config.get("learning", {})
    alpha = learning_cfg.get("alpha_rules_weight", 0.7)

    if learning_cfg.get("enable", True) and MODEL_PATH.exists():
        artifact = joblib.load(MODEL_PATH)
        model = artifact["model"]
        meta = artifact.get("meta", {})
        X, _ = build_feature_matrix(
            df,
            top_countries=meta.get("top_countries"),
            top_sources=meta.get("top_sources"),
        )
        df["ModelScore"] = model.predict_proba(X)[:, 1]
        df["ScoreFinal"] = alpha * df["ScoreRules"] + (1 - alpha) * df["ModelScore"]
    else:
        df["ModelScore"] = 0.0
        df["ScoreFinal"] = df["ScoreRules"]

    return df
