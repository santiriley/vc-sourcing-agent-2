import logging
from typing import List

import pandas as pd
import yaml

from .connectors import cse

logger = logging.getLogger(__name__)


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def run(config_path: str = "config.yaml") -> pd.DataFrame:
    """Run sourcing based on configuration flags."""
    config = load_config(config_path)
    frames: List[pd.DataFrame] = []

    if config.get("enable_cse"):
        queries = config.get("cse_queries", [])
        try:
            frames.append(cse.search_cse(queries))
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("CSE connector failed: %s", exc)

    if frames:
        return pd.concat(frames, ignore_index=True)

    return pd.DataFrame(columns=[
        "title", "snippet", "url", "published", "source", "country_guess"
    ])


if __name__ == "__main__":
    df = run()
    print(df.head())
