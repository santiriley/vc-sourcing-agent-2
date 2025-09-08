"""Filtering and scoring utilities for sourcing leads.

This module evaluates text snippets against rules defined in
``config.yaml``. Each matching rule appends a reason code and
contributes to the overall score. Thresholds in the configuration
determine whether the item is routed to ``Leads``, ``Review`` or
``Discard``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
import yaml
import pathlib


def load_config(path: str | None = None) -> Dict:
    """Load YAML configuration.

    Parameters
    ----------
    path: str | None
        Optional path to the configuration file. Defaults to ``config.yaml``
        in the repository root.
    """
    cfg_path = pathlib.Path(path or "config.yaml")
    with cfg_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@dataclass
class Result:
    """Outcome from applying filters to a text blob."""

    score: int
    route: str
    reasons: List[str]


def _contains_any(text: str, terms: List[str]) -> bool:
    lower = text.lower()
    return any(term.lower() in lower for term in terms)


def evaluate_text(text: str, config: Dict) -> Result:
    """Score text using ``config`` rules.

    Parameters
    ----------
    text:
        The text to evaluate.
    config:
        Parsed configuration dictionary.
    """
    weights = config.get("weights", {})
    rules = config.get("rules", {})
    thresholds = config.get("thresholds", {})

    reasons: List[str] = []
    score = 0

    # Must have revenue indicator
    revenue_rule = rules.get("must_have_any", {}).get("revenue_indicators", {})
    if _contains_any(text, revenue_rule.get("terms", [])):
        reasons.append(revenue_rule.get("reason", "POST_REVENUE"))
        score += weights.get("post_revenue", 0)
    else:
        return Result(score=0, route="Discard", reasons=[revenue_rule.get("missing_reason", "NO_REVENUE")])

    # Must have geography
    geo_rule = rules.get("must_have_geo", {})
    if _contains_any(text, geo_rule.get("countries", [])):
        reasons.append(geo_rule.get("reason", "GEO"))
        score += weights.get("geo", 0)
    else:
        return Result(score=0, route="Discard", reasons=[geo_rule.get("missing_reason", "NO_GEO")])

    # Exclusion terms
    exclude_rule = rules.get("exclude_if_any", {})
    if _contains_any(text, exclude_rule.get("terms", [])):
        return Result(score=0, route="Discard", reasons=[exclude_rule.get("reason", "EXCLUDE")])

    # Sector penalties
    fintech_rule = rules.get("sector_penalties", {}).get("fintech", {})
    if _contains_any(text, fintech_rule.get("terms", [])):
        reasons.append(fintech_rule.get("reason", "FINTECH"))
        score += weights.get("fintech_penalty", 0)

    # Enterprise bonus
    enterprise_rule = rules.get("enterprise_bonus", {})
    if _contains_any(text, enterprise_rule.get("terms", [])):
        reasons.append(enterprise_rule.get("reason", "ENTERPRISE"))
        score += weights.get("enterprise", 0)

    # Female founder bonus
    female_rule = rules.get("female_bonus", {})
    if _contains_any(text, female_rule.get("terms", [])):
        reasons.append(female_rule.get("reason", "FEMALE_FOUNDER"))
        score += weights.get("female", 0)

    # Routing
    if score >= thresholds.get("min_score_to_append", 0):
        route = "Leads"
    elif score >= thresholds.get("min_score_to_review", 0):
        route = "Review"
    else:
        route = "Discard"

    return Result(score=score, route=route, reasons=reasons)
