import logging
from typing import Any

import pandas as pd
import yaml

from .connectors.cse import search_cse


def load_config(path: str) -> dict:
    with open(path, "r") as fh:
        return yaml.safe_load(fh)


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """Basic de-duplication by URL."""
    return df.drop_duplicates(subset="url", keep="first")


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Placeholder for filters/scoring. Currently a no-op."""
    return df


def run(cfg: dict) -> pd.DataFrame:
    """Run sourcing based on configuration."""
    items = pd.DataFrame(columns=["title", "snippet", "url", "source", "country_guess"])

    if cfg.get("enable_cse"):
        cse_df = search_cse(cfg.get("cse_queries", []))
        items = pd.concat([items, cse_df], ignore_index=True)

    items = dedupe(items)
    items = apply_filters(items)
    return items


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = load_config("config.yaml")
    result_df = run(config)
    print(result_df.head())
