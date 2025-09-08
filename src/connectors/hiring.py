import json
import math
import os
from typing import Dict, Optional

import requests
from urllib.parse import urlparse

DEFAULT_TIMEOUT = 5
CACHE_DIR = "/tmp/hiring_cache"

ROLE_KEYWORDS = {
    "sales": ["sales", "account executive", "business development"],
    "cs": ["customer success", "support", "customer support"],
    "eng": ["engineer", "developer", "engineering"],
}


def _guess_slug(domain: Optional[str] = None, title: Optional[str] = None) -> Optional[str]:
    """Best-effort slug guess from domain or title."""
    if domain:
        try:
            hostname = urlparse(f"http://{domain}").hostname or ""
            return hostname.split(".")[0]
        except Exception:
            pass
    if title:
        return "".join(ch for ch in title.lower() if ch.isalnum())
    return None


def _categorize(title: str) -> Dict[str, int]:
    counts = {k: 0 for k in ROLE_KEYWORDS}
    lt = title.lower()
    for key, kws in ROLE_KEYWORDS.items():
        if any(kw in lt for kw in kws):
            counts[key] += 1
    return counts


def estimate_open_roles(
    company_slug: Optional[str] = None,
    domain: Optional[str] = None,
    title: Optional[str] = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    guess_slug_from_domain: bool = True,
) -> Dict[str, object]:
    """Estimate open roles from public job boards.

    Returns a dict with open_roles and role_breakdown. Failures are silent
    and return zeros.
    """

    slug = company_slug
    if not slug and guess_slug_from_domain:
        slug = _guess_slug(domain, title)
    if not slug:
        return {"open_roles": 0, "role_breakdown": {}}

    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(CACHE_DIR, f"{slug}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as f:
                return json.load(f)
        except Exception:
            pass

    headers = {"User-Agent": "Mozilla/5.0"}
    result = {"open_roles": 0, "role_breakdown": {k: 0 for k in ROLE_KEYWORDS}}

    urls = [
        f"https://boards.greenhouse.io/{slug}/jobs?format=json",
        f"https://api.lever.co/v0/postings/{slug}?mode=json",
    ]

    for url in urls:
        try:
            resp = requests.get(url, timeout=timeout, headers=headers)
            if resp.status_code != 200:
                continue
            data = resp.json()
            jobs = data.get("jobs") if isinstance(data, dict) else data
            if not isinstance(jobs, list):
                continue
            result["open_roles"] = len(jobs)
            for job in jobs:
                title = job.get("title") or job.get("text") or ""
                counts = _categorize(title)
                for k, v in counts.items():
                    result["role_breakdown"][k] += v
            break
        except requests.RequestException:
            continue
        except ValueError:
            continue

    try:
        with open(cache_file, "w") as f:
            json.dump(result, f)
    except Exception:
        pass
    return result
