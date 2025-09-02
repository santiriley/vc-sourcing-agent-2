"""Utilities to remove near-duplicate rows.

The deduplication strategy is intentionally lightweight: it keeps the
row with the highest ``score`` and most recent ``timestamp`` when either
of the following conditions hold:

* URLs match exactly.
* ``company`` and ``title`` are very similar according to rapidfuzz's
  token sort ratio (>=90).

The module exposes a single :func:`dedupe_rows` function that accepts a
list of dictionaries and returns a new list with duplicates removed.
"""
from __future__ import annotations

from typing import Dict, Iterable, List

try:  # pragma: no cover - best effort when rapidfuzz missing
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    class _Fallback:
        @staticmethod
        def token_sort_ratio(a: str, b: str) -> int:
            return 100 if a == b else 0
    fuzz = _Fallback()


def _better(new: Dict, existing: Dict) -> bool:
    """Return True if ``new`` should replace ``existing``."""
    if new.get("score", 0) != existing.get("score", 0):
        return new.get("score", 0) > existing.get("score", 0)
    return new.get("timestamp", "") > existing.get("timestamp", "")


def dedupe_rows(rows: Iterable[Dict], title_thresh: int = 90) -> List[Dict]:
    """Merge near-duplicate entries.

    Parameters
    ----------
    rows:
        Iterable of dictionaries representing leads.
    title_thresh:
        Similarity threshold for matching titles/companies.  Defaults to
        90 which is conservative but effective for noisy data.
    """

    deduped: List[Dict] = []
    for row in rows:
        row_url = row.get("url")
        row_title = (row.get("title") or "").lower()
        row_company = (row.get("company") or "").lower()

        replacement_idx = None
        for idx, existing in enumerate(deduped):
            ex_url = existing.get("url")
            if row_url and ex_url and row_url == ex_url:
                replacement_idx = idx
                break

            ex_title = (existing.get("title") or "").lower()
            ex_company = (existing.get("company") or "").lower()
            if (
                fuzz.token_sort_ratio(row_title, ex_title) >= title_thresh
                and fuzz.token_sort_ratio(row_company, ex_company) >= title_thresh
            ):
                replacement_idx = idx
                break

        if replacement_idx is None:
            deduped.append(row)
        elif _better(row, deduped[replacement_idx]):
            deduped[replacement_idx] = row

    return deduped
