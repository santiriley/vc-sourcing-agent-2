import os
import sys
import pandas as pd
import yaml

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.sourcing import route


def test_routing(tmp_path):
    config = yaml.safe_load(open('config.yaml'))
    df = pd.DataFrame([
        {
            'Company': 'Acme',
            'URL': 'http://a',
            'Source': 'feed',
            'Country': 'mexico',
            'Title': 'Acme hits $1M revenue',
            'Snippet': 'company with strong revenue growth',
            'Signals': '',
            'Date': '2024-01-01',
            'Run ID': '1',
        },
        {
            'Company': 'Beta',
            'URL': 'http://b',
            'Source': 'feed',
            'Country': 'mexico',
            'Title': 'Beta launches new product',
            'Snippet': 'launch in mexico',
            'Signals': '',
            'Date': '2024-01-01',
            'Run ID': '1',
        },
        {
            'Company': 'Gamma',
            'URL': 'http://c',
            'Source': 'feed',
            'Country': 'mexico',
            'Title': 'Gamma pre-revenue idea',
            'Snippet': 'pre-revenue hackathon project',
            'Signals': '',
            'Date': '2024-01-01',
            'Run ID': '1',
        },
    ])

    leads, review = route(df, config=config)

    assert leads['Company'].tolist() == ['Acme']
    assert review['Company'].tolist() == ['Beta']

    assert os.path.exists('data/new_rows.csv')
    new_rows = pd.read_csv('data/new_rows.csv')
    assert new_rows['Company'].tolist() == ['Acme']
    # cleanup
    os.remove('data/new_rows.csv')
