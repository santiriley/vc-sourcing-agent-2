import pandas as pd
from typing import List, Tuple, Dict, Any

# Columns used for binary flags
BINARY_FIELDS = [
    "geo",
    "post_revenue",
    "enterprise",
    "fintech",
    "explicit_female_founder_phrase",
]

# Columns used for numeric features
NUMERIC_FIELDS = ["ScoreRules", "OpenRoles", "PressMentions"]

def _top_categories(series: pd.Series, top_n: int) -> List[str]:
    """Return the most frequent `top_n` entries from a Series."""
    return series.value_counts().nlargest(top_n).index.tolist()

def build_feature_matrix(
    df: pd.DataFrame,
    *,
    top_countries: List[str] | None = None,
    top_sources: List[str] | None = None,
    top_n: int = 10,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Create a feature matrix from raw lead rows.

    Parameters
    ----------
    df : DataFrame containing raw lead information.
    top_countries, top_sources : optional lists of most common categories to
        one-hot encode. If not provided they are inferred from ``df``.
    top_n : maximum number of countries/sources to one-hot encode when inferring.

    Returns
    -------
    features : DataFrame
        Numeric feature matrix suitable for model training/inference.
    meta : dict
        Contains the lists of ``top_countries`` and ``top_sources`` used, which
        are required to reproduce the feature matrix on unseen data.
    """
    features = pd.DataFrame(index=df.index)

    # Binary indicators
    for col in BINARY_FIELDS:
        features[col] = df.get(col, 0).fillna(0).astype(int)

    # Numeric columns
    for col in NUMERIC_FIELDS:
        features[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

    # Determine category columns
    country_col = "Country" if "Country" in df.columns else "country" if "country" in df.columns else None
    source_col = "Source" if "Source" in df.columns else "source" if "source" in df.columns else None

    if top_countries is None and country_col:
        top_countries = _top_categories(df[country_col], top_n)
    if top_sources is None and source_col:
        top_sources = _top_categories(df[source_col], top_n)

    # One-hot for countries
    if country_col:
        countries = df[country_col].fillna("other")
        for c in top_countries or []:
            features[f"country_{c}"] = (countries == c).astype(int)
        features["country_other"] = (~countries.isin(top_countries or [])).astype(int)

    # One-hot for sources
    if source_col:
        sources = df[source_col].fillna("other")
        for s in top_sources or []:
            features[f"source_{s}"] = (sources == s).astype(int)
        features["source_other"] = (~sources.isin(top_sources or [])).astype(int)

    meta = {
        "top_countries": top_countries or [],
        "top_sources": top_sources or [],
    }
    return features, meta
