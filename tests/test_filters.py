from pathlib import Path
import sys

import pytest

# Ensure the package root is importable when using the src layout
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.sourcing import Row, classify_row, load_config

CONFIG = load_config(ROOT / "config.yaml")


def test_lead_routing():
    row = Row(title="Startup post-revenue in Chile", snippet="", url="http://example.com")
    assert classify_row(row, CONFIG) == "lead"
    assert row.score >= 4


def test_review_missing_geo():
    row = Row(title="Post-revenue startup expanding", snippet="", url="http://example.com")
    assert classify_row(row, CONFIG) == "review"


def test_review_missing_postrevenue():
    row = Row(title="Startup in Chile raises seed", snippet="", url="http://example.com")
    assert classify_row(row, CONFIG) == "review"


def test_excluded_term_drop():
    row = Row(title="Idea stage project in Chile", snippet="", url="http://example.com")
    assert classify_row(row, CONFIG) == "drop"


def test_fintech_penalty():
    row = Row(title="Fintech post-revenue in Brazil", snippet="", url="http://example.com")
    classification = classify_row(row, CONFIG)
    assert classification == "lead"
    assert row.score == 4
