import os
import time
import logging
from typing import List

import requests
import pandas as pd

COUNTRIES = [
    "Costa Rica","Guatemala","El Salvador","Honduras","Nicaragua","Panama",
    "Belize","Colombia","Venezuela","Ecuador","Peru","Bolivia","Chile",
    "Argentina","Uruguay","Paraguay","Brazil"
]


def guess_country(text: str) -> str | None:
    text_lower = text.lower()
    for country in COUNTRIES:
        if country.lower() in text_lower:
            return country
    return None


def search_cse(queries: List[str], max_results: int = 10) -> pd.DataFrame:
    """Query the Google Custom Search API and return results as a DataFrame.

    Returns a DataFrame with columns: title, snippet, url, source, country_guess."""
    api_key = os.environ.get("CSE_API_KEY")
    cx = os.environ.get("CSE_CX")
    if not api_key or not cx:
        logging.warning("CSE disabled: missing CSE_API_KEY or CSE_CX")
        return pd.DataFrame(columns=["title","snippet","url","source","country_guess"])

    session = requests.Session()
    all_items: list[dict] = []
    for query in queries:
        fetched = 0
        start = 1
        while fetched < max_results:
            params = {
                "key": api_key,
                "cx": cx,
                "q": query,
                "start": start,
            }
            try:
                resp = session.get(
                    "https://www.googleapis.com/customsearch/v1",
                    params=params,
                    timeout=30,
                )
                if resp.status_code != 200:
                    logging.warning("CSE error %s: %s", resp.status_code, resp.text)
                    break
                data = resp.json()
            except requests.RequestException as exc:
                logging.warning("CSE request failed: %s", exc)
                break

            for item in data.get("items", []):
                combined = f"{item.get('title','')} {item.get('snippet','')}"
                all_items.append(
                    {
                        "title": item.get("title", ""),
                        "snippet": item.get("snippet", ""),
                        "url": item.get("link", ""),
                        "source": "google_cse",
                        "country_guess": guess_country(combined),
                    }
                )
                fetched += 1
                if fetched >= max_results:
                    break

            queries_info = data.get("queries", {})
            next_pages = queries_info.get("nextPage")
            if not next_pages or fetched >= max_results:
                break
            start = next_pages[0].get("startIndex", 0)
            time.sleep(1)
        time.sleep(1)

    return pd.DataFrame(all_items, columns=["title","snippet","url","source","country_guess"])
