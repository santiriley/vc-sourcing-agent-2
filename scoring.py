import yaml

CENTRAL_AMERICA = {
    'Belize', 'Costa Rica', 'El Salvador', 'Guatemala', 'Honduras', 'Nicaragua', 'Panama'
}
SOUTH_AMERICA = {
    'Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Guyana',
    'Paraguay', 'Peru', 'Suriname', 'Uruguay', 'Venezuela'
}
FINTECH_KEYWORDS = {
    'fintech', 'financial technology', 'banking', 'payments', 'lending',
    'credit', 'debit', 'neobank'
}


def load_config(path: str = 'config.yaml') -> dict:
    """Load YAML configuration from *path*."""
    with open(path, 'r', encoding='utf-8') as fh:
        return yaml.safe_load(fh)


def score_startup(startup: dict, config: dict) -> float:
    """Score a startup dictionary based on simple heuristics.

    Parameters
    ----------
    startup:
        Dictionary with optional keys ``location``, ``description`` and
        ``founder_genders`` (list of strings).
    config:
        Configuration dictionary loaded from :func:`load_config`.
    """
    weights = config.get('weights', {})
    score = 0.0

    location = startup.get('location', '')
    location_lower = location.lower()
    if any(country.lower() in location_lower for country in CENTRAL_AMERICA):
        score += weights.get('central_america_presence', 0)
    elif any(country.lower() in location_lower for country in SOUTH_AMERICA):
        score += weights.get('south_of_mexico_presence', 0)

    description = startup.get('description', '')
    description_lower = description.lower()
    if any(keyword in description_lower for keyword in FINTECH_KEYWORDS):
        score += weights.get('fintech_keyword_penalty', 0)

    genders = startup.get('founder_genders', [])
    genders_lower = [g.lower() for g in genders]
    if any(g in {'female', 'woman', 'women'} for g in genders_lower):
        score += weights.get('female_founder_boost', 0)

    return score


if __name__ == '__main__':
    cfg = load_config()
    demo_startup = {
        'location': 'Costa Rica',
        'description': 'A fintech platform empowering women-led businesses',
        'founder_genders': ['female']
    }
    print('Demo score:', score_startup(demo_startup, cfg))
