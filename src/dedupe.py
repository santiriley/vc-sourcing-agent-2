"""Utilities for aggressive deduplication of company stories."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

import pandas as pd
from rapidfuzz import fuzz


@dataclass
class DedupeConfig:
    """Configuration for deduplication."""

    url_col: str = "URL"
    company_col: str = "Company"
    title_col: str = "Title"
    score_col: str = "Score"
    date_col: str = "Date"
    threshold: int = 90


def _select_best(df: pd.DataFrame, cfg: DedupeConfig) -> pd.DataFrame:
    """Return dataframe sorted to prioritise highest score and newest date."""

    if cfg.score_col in df.columns and cfg.date_col in df.columns:
        return (
            df.sort_values([cfg.score_col, cfg.date_col], ascending=[False, False])
            .reset_index(drop=True)
        )
    return df.reset_index(drop=True)


def dedupe(df: pd.DataFrame, cfg: Optional[DedupeConfig] = None) -> pd.DataFrame:
    """Deduplicate stories.

    Steps:
        * Exact dedupe on URL keeping highest score / newest date.
        * Fuzzy dedupe on company name or title using RapidFuzz's token sort ratio.
          Rows are grouped into clusters of near duplicates; the best row from each
          cluster is retained.  All retained rows include a ``cluster_id`` to show
          which stories were near duplicates.

    Parameters
    ----------
    df:
        DataFrame containing at least the columns configured in ``cfg``.
    cfg:
        Optional :class:`DedupeConfig` to override column names or threshold.

    Returns
    -------
    pandas.DataFrame
        Deduplicated DataFrame with an added ``cluster_id`` column.
    """

    cfg = cfg or DedupeConfig()

    data = _select_best(df, cfg)

    # Exact dedupe on URL
    if cfg.url_col in data.columns:
        data = data.drop_duplicates(subset=[cfg.url_col], keep="first").reset_index(drop=True)

    clusters: List[pd.Series] = []
    cluster_ids: List[int] = []
    next_cluster_id = 1

    for _, row in data.iterrows():
        matched = False
        for cid, rep in clusters:
            comp_ratio = 0
            title_ratio = 0
            if cfg.company_col in data.columns and pd.notna(row.get(cfg.company_col)) and pd.notna(rep.get(cfg.company_col)):
                comp_ratio = fuzz.token_sort_ratio(row[cfg.company_col], rep[cfg.company_col])
            if cfg.title_col in data.columns and pd.notna(row.get(cfg.title_col)) and pd.notna(rep.get(cfg.title_col)):
                title_ratio = fuzz.token_sort_ratio(row[cfg.title_col], rep[cfg.title_col])

            if max(comp_ratio, title_ratio) >= cfg.threshold:
                cluster_ids.append(cid)
                matched = True
                break

        if not matched:
            clusters.append((next_cluster_id, row))
            cluster_ids.append(next_cluster_id)
            next_cluster_id += 1

    data = data.assign(cluster_id=cluster_ids)
    # Keep only the best row per cluster (data already sorted by score/date)
    deduped = data.drop_duplicates(subset=["cluster_id"], keep="first").reset_index(drop=True)

    return deduped


__all__ = ["DedupeConfig", "dedupe"]
