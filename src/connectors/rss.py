import feedparser
from datetime import datetime
from typing import Dict, List


def fetch_rss_items(urls: List[str]) -> List[Dict[str, str]]:
    """Fetch and normalize items from a list of RSS feeds.

    Args:
        urls: A list of RSS feed URLs.

    Returns:
        A list of dictionaries containing normalized fields: ``title``, ``link``,
        ``summary``, ``published`` (ISO 8601 string) and ``source``.
    """
    items: List[Dict[str, str]] = []
    seen_links: set[str] = set()

    for url in urls:
        feed = feedparser.parse(url)
        source_title = feed.feed.get("title", "")

        for entry in feed.entries:
            link = entry.get("link")
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            published = entry.get("published", "")
            if entry.get("published_parsed"):
                try:
                    published = datetime(*entry.published_parsed[:6]).isoformat()
                except Exception:
                    pass

            items.append(
                {
                    "title": entry.get("title", ""),
                    "link": link,
                    "summary": entry.get("summary", ""),
                    "published": published,
                    "source": source_title,
                }
            )

    return items
