import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import scoring


def test_geo_and_revenue():
    text = "The company reported annual recurring revenue growth."
    assert scoring.score("Brazil", text) == 6


def test_female_and_fintech_penalty():
    text = "A female-founded fintech launched in Mexico."
    assert scoring.score("Mexico", text) == 3


def test_enterprise_keyword():
    text = "Argentina enterprise B2B startup reaches revenue milestones."
    assert scoring.score("Argentina", text) == 7
