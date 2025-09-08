"""Main sourcing utilities."""
from __future__ import annotations

import os
from typing import Dict, Tuple

import pandas as pd
import yaml

from .filters import apply_filters
from .dedupe import cluster_and_keep_top


COLUMNS = [
    "Company",
    "URL",
    "Source",
    "Country",
    "Title",
    "Snippet",
    "Signals",
    "Score",
    "Reason Codes",
    "Date",
    "Run ID",
]


def route(df: pd.DataFrame, config: Dict | None = None, config_path: str = "config.yaml") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Score, dedupe and route rows to Leads or Review."""
    if config is None:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

    df = apply_filters(df, config)

    dedupe_fields = config.get("dedupe_fields", ["Company", "Title"])
    min_ratio = config.get("dedupe_min_ratio", 86)
    deduped, before, after, clusters = cluster_and_keep_top(
        df,
        keys=dedupe_fields,
        min_ratio=min_ratio,
    )
    print(f"[dedupe] kept {after}/{before}; clusters={clusters}")

    min_append = config.get("min_score_to_append", 4)
    min_review = config.get("min_score_to_review", 2)

    leads = deduped[deduped["Score"] >= min_append]
    review = deduped[(deduped["Score"] >= min_review) & (deduped["Score"] < min_append)]

    leads = leads[COLUMNS]
    review = review[COLUMNS]

    if not leads.empty:
        os.makedirs("data", exist_ok=True)
        leads.to_csv("data/new_rows.csv", index=False)

    return leads, review
