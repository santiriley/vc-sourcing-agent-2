from __future__ import annotations
import yaml
from functools import lru_cache
from typing import Any, Dict


@lru_cache()
def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    """Load YAML configuration for scoring."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def score(country: str, text: str, config: Dict[str, Any] | None = None) -> int:
    """Calculate lead score based on signals defined in config.

    Args:
        country: Country associated with the lead.
        text: Text to search for signal keywords.
        config: Optional pre-loaded configuration dictionary.

    Returns:
        Integer score clamped between 0 and 10.
    """
    if config is None:
        config = load_config()
    weights = config["weights"]
    total = 0
    text_lower = text.lower()
    geo_conf = weights["geo"]
    if country in geo_conf["countries"]:
        total += geo_conf["weight"]
    for key in ["post_revenue", "female", "enterprise", "fintech_penalty"]:
        entry = weights[key]
        if any(kw.lower() in text_lower for kw in entry["keywords"]):
            total += entry["weight"]
    return max(0, min(10, total))
