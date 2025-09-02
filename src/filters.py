"""Filtering utilities for sourcing leads.

This module contains small helper functions used to check whether
content includes required signals such as post‑revenue indicators or
country mentions. All checks are case insensitive and operate on plain
strings.
"""
from __future__ import annotations

from typing import Iterable, Sequence


def _normalize(text: str) -> str:
    """Return ``text`` in lower case or an empty string if ``None``."""
    return text.lower() if isinstance(text, str) else ""


def must_have_any(text: str, terms: Sequence[str]) -> bool:
    """Return ``True`` if *any* term is found within ``text``.

    Parameters
    ----------
    text:
        Text to search. ``None`` is treated as an empty string.
    terms:
        Iterable of phrases that should trigger a positive match.
    """

    lower = _normalize(text)
    return any(t.lower() in lower for t in terms)


def must_have_geo(parts: Iterable[str], countries: Sequence[str]) -> bool:
    """Return ``True`` if any country is mentioned in the given text parts.

    ``parts`` is typically an iterable with the title, snippet and url of a
    search result. ``None`` values are ignored.
    """

    combined = " ".join(_normalize(p) for p in parts if p)
    return any(country.lower() in combined for country in countries)


def exclude_if_any(text: str, terms: Sequence[str]) -> bool:
    """Return ``True`` if any of the terms appear in ``text``.

    This helper is used to drop rows that contain noisy phrases such as
    "pre‑revenue" or "hackathon".
    """

    lower = _normalize(text)
    return any(term.lower() in lower for term in terms)


def sector_penalty(text: str, sector_terms: dict[str, Sequence[str]],
                   weights: dict[str, int]) -> int:
    """Return a negative penalty if text matches a black‑listed sector.

    At the moment the only supported sector is ``fintech``.  The penalty
    value is retrieved from ``weights['fintech_penalty']`` so that the
    configuration file controls its magnitude.
    """

    lower = _normalize(text)
    penalty = 0

    fintech_terms = sector_terms.get("fintech", [])
    if any(t.lower() in lower for t in fintech_terms):
        penalty += weights.get("fintech_penalty", 0)

    return penalty
