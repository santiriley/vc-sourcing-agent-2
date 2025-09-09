"""Sourcing utilities with optional ML scoring."""
from __future__ import annotations

import os
import pathlib
from typing import Any

import yaml

from . import features


DEFAULT_CONFIG_PATH = pathlib.Path("config.yaml")
DEFAULT_MODEL_PATH = pathlib.Path("model/model.joblib")


def apply_scores(
    df,
    config_path: str | os.PathLike[str] = DEFAULT_CONFIG_PATH,
    model_path: str | os.PathLike[str] = DEFAULT_MODEL_PATH,
):
    """Return dataframe with blended rule/model scores.

    The function reads learning parameters from ``config.yaml`` and
    blends the ``rules_score`` column with predictions from an optional
    ML model. If the model or its dependencies are missing, scoring
    falls back to rules only and a single log line is emitted.
    """

    with open(config_path, "r", encoding="utf-8") as fh:
        cfg: dict[str, Any] = yaml.safe_load(fh) or {}

    learning = cfg.get("learning", {})
    enable = bool(learning.get("enable", True))
    alpha = float(learning.get("alpha_rules_weight", 0.7))

    df = df.copy()
    rules = df.get("rules_score", 0).astype(float)

    if not enable:
        df["FinalScore"] = rules.clip(0, 10)
        return df

    try:
        from joblib import load  # type: ignore

        model = load(model_path)
    except Exception:
        print("[ml] No model found; using rules-only scoring.")
        df["FinalScore"] = rules.clip(0, 10)
        return df

    try:
        feats = features.build_features(df)
        model_score = model.predict_proba(feats)[:, 1]
    except Exception as exc:  # pragma: no cover - defensive
        print(f"[ml] Model inference failed: {exc}; using rules-only scoring.")
        df["FinalScore"] = rules.clip(0, 10)
        return df

    final = alpha * rules + (1 - alpha) * (10 * model_score)
    df["FinalScore"] = final.clip(0, 10)
    return df
