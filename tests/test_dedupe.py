import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.dedupe import cluster_and_keep_top


def test_cluster_and_keep_top():
    df = pd.DataFrame([
        {
            'Company': 'Acme',
            'Title': 'Acme raises seed round',
            'URL': 'http://a1',
            'Score': 5,
            'Date': '2024-01-02',
        },
        {
            'Company': 'Acme',
            'Title': 'Acme raises seed funding',
            'URL': 'http://a2',
            'Score': 3,
            'Date': '2024-01-01',
        },
    ])

    deduped, before, after, clusters = cluster_and_keep_top(df, min_ratio=86)

    assert before == 2
    assert after == 1
    assert clusters == 1
    row = deduped.iloc[0]
    assert row['URL'] == 'http://a1'
    assert row['cluster_id'] == 0
