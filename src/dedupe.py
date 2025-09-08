"""Deduplication utilities."""
from __future__ import annotations

from typing import List, Tuple

import pandas as pd

try:
    from rapidfuzz import fuzz

    def _ratio(a: str, b: str) -> float:
        return fuzz.token_set_ratio(a, b)
except Exception:  # pragma: no cover - fallback
    from difflib import SequenceMatcher

    def _ratio(a: str, b: str) -> float:  # pragma: no cover - fallback
        return SequenceMatcher(None, a, b).ratio() * 100


def cluster_and_keep_top(
    df: pd.DataFrame,
    keys: List[str] | None = None,
    url_col: str = "URL",
    score_col: str = "Score",
    date_col: str = "Date",
    min_ratio: float = 86.0,
) -> Tuple[pd.DataFrame, int, int, int]:
    """Drop exact URL duplicates and cluster near-duplicates.

    Returns the deduped dataframe along with before/after counts and number of
    clusters formed. Rows keep their assigned ``cluster_id``.
    """
    if keys is None:
        keys = ["Company", "Title"]

    df = df.copy()
    before = len(df)

    # remove exact URL duplicates keeping highest score then newest
    df = df.sort_values(by=[score_col, date_col], ascending=[False, False])
    df = df.drop_duplicates(subset=[url_col], keep="first")

    combined = (
        df[keys].fillna("").agg(" ".join, axis=1).str.lower().tolist()
    )

    clusters: List[str] = []
    cluster_ids: List[int] = []

    for text in combined:
        assigned = False
        for idx, rep in enumerate(clusters):
            if _ratio(text, rep) >= min_ratio:
                cluster_ids.append(idx)
                assigned = True
                break
        if not assigned:
            clusters.append(text)
            cluster_ids.append(len(clusters) - 1)

    df["cluster_id"] = cluster_ids
    deduped = df.drop_duplicates(subset=["cluster_id"], keep="first")
    after = len(deduped)
    return deduped, before, after, len(clusters)
