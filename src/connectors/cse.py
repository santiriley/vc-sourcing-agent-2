import logging
import os
import time
from typing import List
from urllib.parse import urlparse

import pandas as pd
import requests

logger = logging.getLogger(__name__)

GOOGLE_CSE_URL = "https://www.googleapis.com/customsearch/v1"


def _guess_country_from_url(url: str) -> str | None:
    """Guess country code from URL's top-level domain."""
    try:
        hostname = urlparse(url).hostname or ""
        tld = hostname.split(".")[-1]
        if len(tld) == 2 and tld.isalpha():
            return tld.upper()
    except Exception:  # pragma: no cover - best effort
        pass
    return None


def _is_linkedin_blocked(url: str) -> bool:
    """Return True if LinkedIn URL responds with a blocking status."""
    try:
        resp = requests.head(url, timeout=5)
        if resp.status_code in (999, 403):
            return True
    except requests.RequestException:
        return True
    return False


def search_cse(queries: List[str], max_results_per_query: int = 10) -> pd.DataFrame:
    """Search Google Custom Search for each query and return normalized rows."""
    columns = ["title", "snippet", "url", "published", "source", "country_guess"]
    api_key = os.getenv("CSE_API_KEY")
    cx = os.getenv("CSE_CX")
    if not api_key or not cx:
        logger.warning("CSE_API_KEY or CSE_CX not set; skipping Google CSE")
        return pd.DataFrame(columns=columns)

    records = []
    for query in queries:
        params = {
            "key": api_key,
            "cx": cx,
            "q": query,
            "num": min(max_results_per_query, 10),
        }
        try:
            resp = requests.get(GOOGLE_CSE_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:  # pragma: no cover - network
            logger.error("CSE request failed for query %r: %s", query, exc)
            time.sleep(1)
            continue

        for item in data.get("items", []):
            url = item.get("link")
            if not url:
                continue
            if "linkedin.com" in url.lower() and _is_linkedin_blocked(url):
                logger.info("Skipping blocked LinkedIn URL: %s", url)
                continue
            records.append(
                {
                    "title": item.get("title"),
                    "snippet": item.get("snippet"),
                    "url": url,
                    "published": None,
                    "source": "google_cse",
                    "country_guess": _guess_country_from_url(url),
                }
            )
        time.sleep(1)  # basic rate limiting

    return pd.DataFrame(records, columns=columns)
