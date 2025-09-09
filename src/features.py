"""Feature engineering utilities."""
from __future__ import annotations

import pandas as pd


BASE_FEATURES = [
    "geo_flag",
    "postrev_flag",
    "enterprise_flag",
    "fintech_flag",
    "female_phrase_flag",
    "rules_score",
    "open_roles",
    "press_mentions",
]


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Return dataframe of model features.

    The function expects `df` to contain the base columns above plus
    `country` and `source`. Missing columns are filled with 0.
    Additionally, one-hot encodings are generated for the ten most
    frequent countries and sources with all other values grouped into
    an "other" bucket.
    """

    working = df.copy()

    for col in BASE_FEATURES + ["country", "source"]:
        if col not in working:
            working[col] = 0

    features = working[BASE_FEATURES].copy()

    top_countries = (
        working["country"].value_counts().head(10).index.tolist()
        if len(working)
        else []
    )
    features["country"] = working["country"].where(
        working["country"].isin(top_countries), "other"
    )
    features = pd.get_dummies(features, columns=["country"], prefix="country")

    top_sources = (
        working["source"].value_counts().head(10).index.tolist()
        if len(working)
        else []
    )
    features["source"] = working["source"].where(
        working["source"].isin(top_sources), "other"
    )
    features = pd.get_dummies(features, columns=["source"], prefix="source")

    return features
