import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from filters import evaluate_text, load_config

config = load_config()


def test_positive_lead():
    text = (
        "La startup tiene clientes de pago en Colombia y un contrato enterprise. "
        "La fundadora lidera el equipo."
    )
    result = evaluate_text(text, config)
    assert result.route == "Leads"
    assert "POST_REVENUE" in result.reasons
    assert "GEO" in result.reasons
    assert "ENTERPRISE" in result.reasons
    assert "FEMALE_FOUNDER" in result.reasons


def test_excluded_phrase():
    text = "Una idea de hackathon pre-revenue en Mexico"
    result = evaluate_text(text, config)
    assert result.route == "Discard"
    assert "EXCLUDE" in result.reasons or "NO_REVENUE" in result.reasons


def test_fintech_penalty():
    text = "Post-revenue wallet startup operating in Peru"
    result = evaluate_text(text, config)
    expected_score = (
        config["weights"]["geo"]
        + config["weights"]["post_revenue"]
        + config["weights"]["fintech_penalty"]
    )
    assert result.score == expected_score
    assert result.route == "Leads"
    assert "FINTECH" in result.reasons
