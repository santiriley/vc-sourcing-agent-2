import math
from typing import Dict, List, Optional

import yaml

from connectors.hiring import estimate_open_roles
from signals.press import press_momentum


def load_config(path: str = "config.yaml") -> Dict[str, object]:
    with open(path) as f:
        return yaml.safe_load(f)


def score_candidates(
    candidates: List[Dict[str, object]],
    press_items: List[Dict[str, object]],
    config: Optional[Dict[str, object]] = None,
) -> List[Dict[str, object]]:
    """Score candidates with optional hiring and press signals."""
    if config is None:
        config = load_config()

    weights = config.get("weights", {})
    hiring_cfg = config.get("hiring", {})
    press_cfg = config.get("press", {})

    scored = []
    hiring_cache: Dict[str, Dict[str, object]] = {}
    press_cache: Dict[str, Dict[str, float]] = {}

    for cand in candidates:
        row = dict(cand)
        base_score = row.get("score", 0)

        # Hiring signal
        hiring_score = 0.0
        open_roles = 0
        slug_key = row.get("company_slug") or row.get("domain") or row.get("company")
        if hiring_cfg.get("enable") and slug_key:
            if slug_key not in hiring_cache:
                hiring_cache[slug_key] = estimate_open_roles(
                    company_slug=row.get("company_slug"),
                    domain=row.get("domain"),
                    title=row.get("company"),
                    timeout=hiring_cfg.get("timeout_s", 5),
                    guess_slug_from_domain=hiring_cfg.get("guess_slug_from_domain", True),
                )
            hiring_info = hiring_cache[slug_key]
            open_roles = hiring_info.get("open_roles", 0)
            hiring_score = min(3.0, math.log1p(open_roles))

        # Press signal
        press_mentions = 0
        press_score = 0.0
        company_name = row.get("company", "")
        if press_cfg.get("enable") and company_name:
            if company_name not in press_cache:
                press_cache[company_name] = press_momentum(
                    company_name,
                    press_items,
                    time_window_days=press_cfg.get("time_window_days", 14),
                )
            press_info = press_cache[company_name]
            press_mentions = press_info["press_mentions"]
            press_score = press_info["press_score"]

        # Update row
        row["OpenRoles"] = open_roles
        row["HiringScore"] = hiring_score
        row["PressMentions"] = press_mentions
        row["PressScore"] = press_score
        row["score"] = (
            base_score
            + weights.get("hiring", 0) * hiring_score
            + weights.get("press", 0) * press_score
        )

        scored.append(row)

    return scored
