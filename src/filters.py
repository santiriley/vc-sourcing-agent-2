"""Filtering and scoring utilities for sourcing."""
from __future__ import annotations

from typing import Dict, Tuple

import yaml


def load_config(path: str = "config.yaml") -> Dict:
    """Load YAML configuration from *path*."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def score_row(row: Dict, config: Dict) -> Tuple[int, str]:
    """Apply rule checks to a row and return (score, reason_codes)."""
    text = f"{row.get('Title', '')} {row.get('Snippet', '')}".lower()
    country = str(row.get("Country", "")).lower()
    weights = config.get("weights", {})

    score = 0
    codes = []

    # exclusion terms are hard filters
    if any(term.lower() in text for term in config.get("exclude_terms", [])):
        codes.append("exclude")
        return 0, ",".join(codes)

    if country in (c.lower() for c in config.get("must_have_geo", [])):
        score += weights.get("geo", 0)
        codes.append("geo")

    if any(term.lower() in text for term in config.get("must_have_any", [])):
        score += weights.get("post_revenue", 0)
        codes.append("post_revenue")

    if any(term.lower() in text for term in config.get("enterprise_bonus", [])):
        score += weights.get("enterprise", 0)
        codes.append("enterprise")

    if any(term.lower() in text for term in config.get("fintech_penalty", [])):
        score += weights.get("fintech_penalty", 0)
        codes.append("fintech_penalty")

    return score, ",".join(codes)


def apply_filters(df, config: Dict):
    """Apply filters to a dataframe and add Score and Reason Codes columns."""
    results = df.apply(lambda row: score_row(row, config), axis=1, result_type="expand")
    df = df.copy()
    df["Score"] = results[0]
    df["Reason Codes"] = results[1]
    return df
