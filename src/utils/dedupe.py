import pandas as pd
from rapidfuzz import fuzz


def dedupe(df: pd.DataFrame,
           company_col: str = "Company",
           url_col: str = "URL",
           score_col: str = "Score",
           threshold: int = 90) -> pd.DataFrame:
    """Deduplicate a DataFrame of company URLs.

    Rows are sorted by ``score_col`` descending so that duplicates keep the
    highest score. The function then removes exact duplicates on ``url_col``
    and fuzzy duplicates on ``company_col`` using RapidFuzz's
    :func:`token_sort_ratio`.

    Parameters
    ----------
    df : pandas.DataFrame
        Input data containing company information.
    company_col, url_col, score_col : str, optional
        Column names for company, URL and score.
    threshold : int, optional
        Token sort ratio threshold to consider company names duplicates.

    Returns
    -------
    pandas.DataFrame
        Deduplicated DataFrame with the highest-scoring rows retained.
    """
    if df.empty:
        return df

    # Sort so that highest score appears first
    df_sorted = df.sort_values(by=score_col, ascending=False)

    # Exact dedupe on URL
    df_dedup = df_sorted.drop_duplicates(subset=[url_col], keep="first")

    unique_companies = []
    rows = []
    for _, row in df_dedup.iterrows():
        company = str(row[company_col])
        if all(fuzz.token_sort_ratio(company, existing) < threshold
               for existing in unique_companies):
            unique_companies.append(company)
            rows.append(row)

    return pd.DataFrame(rows).reset_index(drop=True)
