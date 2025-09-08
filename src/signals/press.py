import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from dateutil import parser


def press_momentum(
    company: str,
    items: List[Dict[str, object]],
    *,
    time_window_days: int = 14,
) -> Dict[str, float]:
    """Compute press momentum for a company."""
    if not company:
        return {"press_mentions": 0, "press_score": 0.0}

    now = datetime.utcnow()
    window_start = now - timedelta(days=time_window_days)

    mentions = 0
    for item in items or []:
        if item.get("company", "").lower() != company.lower():
            continue
        date_val: Optional[datetime] = None
        raw_date = item.get("date") or item.get("published")
        if isinstance(raw_date, datetime):
            date_val = raw_date
        elif raw_date:
            try:
                date_val = parser.parse(str(raw_date))
            except (parser.ParserError, TypeError, ValueError):
                date_val = None
        if date_val and date_val >= window_start:
            mentions += 1

    press_score = min(3.0, math.log1p(mentions))
    return {"press_mentions": mentions, "press_score": press_score}
