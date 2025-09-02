"""Scoring and routing of potential leads.

The module exposes helpers that take a list of candidate rows and decide
whether they should be appended to the main ``Leads`` worksheet, routed
to a ``Review`` worksheet, or discarded.

Only configuration loading and pure-Python logic live here so the module
is easily testable; interactions with external services (Google Sheets,
feeds, etc.) remain elsewhere in the project.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import yaml

from . import dedupe, filters


@dataclass
class Row:
    title: str
    snippet: str
    url: str
    company: str | None = None
    timestamp: str | None = None
    score: int | None = None


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def score_row(row: Row, config: Dict) -> Tuple[int, bool, bool, bool, int]:
    """Return a tuple with the score and individual feature flags.

    Returns ``(score, postrev, geo, enterprise, penalty)``.
    """

    text_parts = [row.title, row.snippet, row.url]
    combined_text = " ".join(p for p in text_parts if p)

    postrev = filters.must_have_any(combined_text, config.get("post_revenue_terms", []))
    geo = filters.must_have_geo(text_parts, config.get("countries", []))
    enterprise = filters.must_have_any(combined_text, config.get("enterprise_signal_terms", []))
    penalty = filters.sector_penalty(combined_text, config.get("sector_blacklist_terms", {}), config.get("weights", {}))

    weights = config.get("weights", {})
    score = (
        geo * weights.get("geo", 0)
        + postrev * weights.get("post_revenue", 0)
        + enterprise * weights.get("enterprise", 0)
        + penalty
    )
    return score, postrev, geo, enterprise, penalty


def classify_row(row: Row, config: Dict) -> str:
    score, postrev, geo, enterprise, penalty = score_row(row, config)
    row.score = score

    if filters.exclude_if_any(
        f"{row.title} {row.snippet}", config.get("exclude_terms", [])
    ):
        return "drop"

    min_append = config.get("min_score_to_append", 4)
    min_review = config.get("min_score_to_review", 2)

    # Require both a post-revenue indicator and a geo signal to make it
    # directly into Leads. Otherwise route to Review if score is high
    # enough, drop otherwise.
    if postrev and geo and score >= min_append:
        return "lead"
    if score >= min_review:
        return "review"
    return "drop"


def process_rows(rows: Iterable[Dict], config: Dict) -> Tuple[List[Dict], List[Dict]]:
    """Score, dedupe and route ``rows`` according to ``config``.

    ``rows`` should be an iterable of dictionaries with at least ``title``,
    ``snippet`` and ``url`` keys.  The function returns ``(leads, review)``
    lists.  If any leads are appended, they are also written to
    ``data/new_rows.csv`` for downstream processing.
    """

    row_objs = [Row(**r) for r in rows]
    for row in row_objs:
        row.classification = classify_row(row, config)

    deduped = dedupe.dedupe_rows([r.__dict__ for r in row_objs])

    leads: List[Dict] = []
    review: List[Dict] = []
    for row in deduped:
        cls = row.get("classification")
        if cls == "lead":
            leads.append(row)
        elif cls == "review":
            review.append(row)

    if leads:
        Path("data").mkdir(exist_ok=True)
        pd.DataFrame(leads).to_csv("data/new_rows.csv", index=False)

    return leads, review
